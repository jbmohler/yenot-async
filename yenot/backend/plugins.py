import os
import sys
import re
import json
import signal
import asyncio
import logging
import contextlib
import urllib.parse
import traceback
import time
import threading
import queue

import aiohttp.web as web
import asyncpg

from . import misc

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)



class CancelQueue(queue.SimpleQueue):
    def cancel(self):
        self.put(("cancel", None))

    def place_result(self, content):
        self.put(("done", content))

    def wait(self, timeout):
        try:
            x = self.get(timeout=timeout)
        except queue.Empty:
            # TODO: I'm not sure why one would use the wait function or what this exception means in this context
            return

        if x[0] == "cancel":
            raise misc.UserError(
                "cancel-request", "The request was canceled by the client."
            )
        elif x[0] == "done":
            return x[1]
        else:
            raise RuntimeError("CancelQueue elements must be 2-tuples")


async def create_connection(dburl):
    result = urllib.parse.urlsplit(dburl)

    kwargs = {"database": result.path[1:]}
    if result.hostname != None:
        kwargs["host"] = result.hostname
    if result.port != None:
        kwargs["port"] = result.port
    if result.username != None:
        kwargs["user"] = result.username
    if result.password != None:
        kwargs["password"] = result.password

    return await asyncpg.connect(**kwargs)


async def create_pool(dburl):
    result = urllib.parse.urlsplit(dburl)

    kwargs = {"database": result.path[1:]}
    if result.hostname not in [None, ""]:
        kwargs["host"] = result.hostname
    if result.port != None:
        kwargs["port"] = result.port
    if result.username != None:
        kwargs["user"] = result.username
    if result.password != None:
        kwargs["password"] = result.password

    return await asyncpg.create_pool(min_size=3, max_size=6, **kwargs)


# to convert
# - add async on def
# - add await on sql_tab2
# - add param of request
# - return YenotResult


async def yenot_handler(request, handler):
    response = await handler(*args)
    return ...


class YenotApplication:
    def __init__(self, dburl):
        #self.routes = web.RouteTableDef()

        self.app = web.Application()
        #self.app.add_routes(self.routes)

        # create_pool(dburl)
        self._pool = None
        self.dburl = dburl
        self.dbconn_register = {}

        self.sitevars = {}

    def _decorator(self, f, method, route, name, **kwargs):
        logger.info(f"adding {method} {route} -- {f}")
        route = self.app.router.add_route(method, route, f, name=name)
        logger.debug(dir(route))

    def get(self, route, name, **kwargs):
        def closure(f):
            self._decorator(f, "GET", route, name, **kwargs)

        return closure

    def put(self, route, name, **kwargs):
        def closure(f):
            self._decorator(f, "PUT", route, name, **kwargs)

        return closure

    def post(self, route, name, **kwargs):
        def closure(f):
            self._decorator(f, "POST", route, name, **kwargs)

        return closure

    def delete(self, route, name, **kwargs):
        def closure(f):
            self._decorator(f, "DELETE", route, name, **kwargs)

        return closure

    def patch(self, route, name, **kwargs):
        def closure(f):
            self._decorator(f, "PATCH", route, name, **kwargs)

        return closure

    # to become a method of app
    @contextlib.contextmanager
    def cancel_queue(self):
        conn = CancelQueue()
        ctoken = getattr(request, "cancel_token", None)
        try:
            if ctoken != None:
                self.register_connection(ctoken, conn)
            yield conn
        finally:
            if ctoken != None:
                self.unregister_connection(ctoken, conn)

    @contextlib.asynccontextmanager
    async def dbconn(self):
        if not self._pool:
            self._pool = await create_pool(self.dburl)
        conn = await self._pool.acquire()
        try:
            yield conn
        finally:
            await self._pool.release(conn)

    # @contextlib.contextmanager
    # def dbconn(self):
    #     conn = await self.pool.acquire()
    #     ctoken = getattr(request, "cancel_token", None)
    #     try:
    #         if ctoken != None:
    #             self.register_connection(ctoken, conn)
    #         yield conn
    #     finally:
    #         if ctoken != None:
    #             self.unregister_connection(ctoken, conn)
    #         await self.pool.release(conn)

    # to become a method of app
    @contextlib.contextmanager
    async def background_dbconn(self):
        conn = await self.pool.acquire()
        try:
            yield conn
        finally:
            await self.pool.release(conn)

    def register_connection(self, ctoken, conn):
        if ctoken in self.dbconn_register:
            self.dbconn_register[ctoken].append(conn)
        else:
            self.dbconn_register[ctoken] = [conn]

    def unregister_connection(self, ctoken, conn):
        if ctoken in self.dbconn_register:
            self.dbconn_register[ctoken].remove(conn)
            if len(self.dbconn_register[ctoken]) == 0:
                del self.dbconn_register[ctoken]

    def cancel_request(self, cancel_token):
        if cancel_token in self.dbconn_register:
            connections = self.dbconn_register[cancel_token]
            for conn in connections:
                conn.cancel()
        else:
            raise misc.UserError(
                "invalid-param",
                "This is not a recognized request or not capable of being canceled.",
            )

    def delayed_shutdown(self):
        def make_it_stop():
            time.sleep(0.3)
            self.pool.closeall()
            self._paste_server.stop()

        self.stop_thread = threading.Thread(target=make_it_stop)
        self.stop_thread.start()

    def request_content_title(self):
        return request.route.name

    def add_sitevars(self, sitevars):
        for c in sitevars:
            key, value = c.split("=")
            self.sitevars[key] = value

    async def _start(self):
        logger.info(f"server startup on http://{self.run_args['host']}:{self.run_args['port']}")

        self.runner = web.AppRunner(self.app)
        await self.runner.setup()
        site = web.TCPSite(self.runner, self.run_args["host"], self.run_args["port"])
        await site.start()

    async def _stop(self, sig):
        await self.runner.cleanup()

        asyncio.get_event_loop().stop()

    def run(self):
        # web.run_app(self.app)

        loop = asyncio.get_event_loop()

        # May want to catch other signals too
        signals = (signal.SIGTERM, signal.SIGINT)
        for s in signals:
            loop.add_signal_handler(s, lambda s=s: asyncio.create_task(self._stop(s)))

        loop.create_task(self._start())
        loop.run_forever()


global_app = None


def init_application(dburl):
    global global_app

    app = YenotApplication(dburl)

    global_app = app

    # app.install(RequestCancelTracker())
    # app.install(ExceptionTrapper())

    # hook up the basic stuff
    import yenot.server  # noqa: F401

    app.run_args = {
        "host": os.getenv("YENOT_HOST", "0.0.0.0"),
        "port": int(os.getenv("YENOT_PORT", 8080)),
    }

    return app


class RequestCancelTracker:
    name = "yenot-cancel"
    api = 2

    def setup(self, app):
        self.app = app

    def apply(self, callback, route):
        def wrapper(*args, **kwargs):
            request.cancel_token = request.headers.get("X-Yenot-CancelToken", None)
            return callback(*args, **kwargs)

        return wrapper


class ExceptionTrapper:
    name = "yenot-exceptions"
    api = 2

    def setup(self, app):
        # expect app to have dbconn
        self.app = app

    def report(self, e, myresponse, keys):
        with self.app.dbconn() as conn:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            fsumm = [
                (f.filename, f.lineno, f.name)
                for f in traceback.extract_tb(exc_traceback, 15)
            ]
            details = {
                "exc_type": exc_type.__name__,
                "exception": str(exc_value),
                "session": request.headers.get("X-Yenot-SessionID", None),
                "frames": list(reversed(fsumm)),
            }
            des = f"HTTP {myresponse.status} - {keys.get('error-msg', None)}"
            misc.write_event_entry(conn, "Yenot Server Error", des, details)
            conn.commit()

    def apply(self, callback, route):
        def wrapper(*args, **kwargs):
            try:
                return callback(*args, **kwargs)
            except psycopg2.IntegrityError as e:
                if str(e).startswith("duplicate key value violates unique constraint"):
                    match = re.search(
                        r".*Key \([a-zA-Z0-9_]*\)=\((.*)\) already exists.", str(e)
                    )
                    msg = "A duplicate key was found."
                    if match != None:
                        msg = 'A duplicate key with value "{}" was found.'.format(
                            match.group(1)
                        )
                    keys = {"error-key": "duplicate-key", "error-msg": msg}
                elif str(e).startswith("null value in column "):
                    match = re.match(
                        r'null value in column "([a-zA-Z0-9_]*)" violates not-null constraint',
                        str(e).split("\n")[0],
                    )
                    msg = 'The value in field "{}" must be non-empty and valid.'.format(
                        match.group(1)
                    )
                    keys = {"error-key": "null-value", "error-msg": msg}
                else:
                    keys = {
                        "error-key": "data-integrity",
                        "error-msg": "An invalid value was passed to the database.\n\n{}".format(
                            str(e)
                        ),
                    }
                response.status = 403
                self.report(e, response, keys)
                return json.dumps([keys])
            except psycopg2.ProgrammingError as e:
                if bottle.DEBUG:
                    traceback.print_exc()
                    sys.stderr.flush()
                try:
                    prim = e.diag.message_primary
                    lines = e.pgerror.split("\n")
                    if (
                        len(lines) >= 3
                        and lines[1].startswith("LINE")
                        and lines[2].find("^") >= 0
                    ):
                        c2 = lines[2].find("^")
                        s2 = lines[1]
                        sec = s2[:c2] + "##" + s2[c2:]
                    elif len(lines) >= 2:
                        sec = lines[1]
                    else:
                        sec = None
                    if sec == None:
                        errdesc = prim
                    else:
                        errdesc = f"{prim} ({sec})"
                except Exception:
                    errdesc = str(e)
                keys = {
                    "error-key": "sql-syntax-error",
                    "error-msg": f"SQL Error:  {errdesc}",
                }
                response.status = 500
                response.content_type = "application/json; charset=UTF-8"
                self.report(e, response, keys)
                return json.dumps([keys])
            except psycopg2.extensions.QueryCanceledError:
                keys = {"error-key": "cancel", "error-msg": "Client cancelled request"}
                response.status = 403
                return json.dumps([keys])
            except misc.UserError as e:
                keys = {"error-key": e.key, "error-msg": str(e)}
                response.status = 403
                response.content_type = "application/json; charset=UTF-8"
                return json.dumps([keys])
            except bottle.HTTPError:
                raise
            except Exception as e:
                if bottle.DEBUG:
                    traceback.print_exc()
                    sys.stderr.flush()

                keys = {"error-msg": str(e)}
                response.status = 500
                response.content_type = "application/json; charset=UTF-8"
                self.report(e, response, keys)
                return json.dumps([keys])

        return wrapper

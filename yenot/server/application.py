import yenot.backend.api as api

app = api.get_global_app()

import logging

@app.get("/api/pingdb", name="pingdb", skip=["yenot-auth"])
async def get_api_pingdb(request):
    logging.info("some where")
    import aiohttp.web as web

    async with app.dbconn() as conn:
        logging.info("asldfj")

        return web.Response(text="Hello, world")
    #return "."

@app.get("/api/ping", name="ping", skip=["yenot-auth"])
async def get_api_ping(request):
    #logging.info("asldfj")
    import aiohttp.web as web
    return web.Response(text="somewhere over the rainbow")


@app.put("/api/request/cancel", name="api_request_cancel")
async def put_api_request_cancel(request):
    token = request.query.get("token")
    app.cancel_request(token)
    return api.Results().json_out()

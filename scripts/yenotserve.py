#!/usr/bin/env python
import os
import argparse
import importlib
import yenot.backend

if __name__ == "__main__":
    parse = argparse.ArgumentParser("serve a collection of yenot modules")
    parse.add_argument(
        "dburl",
        help="database identifier in url form (e.g. postgresql://user@host/dbname)",
    )
    parse.add_argument(
        "--module",
        action="append",
        default=[],
        help="specify module to import before starting yenot server",
    )
    parse.add_argument(
        "--sitevar", action="append", default=[], help="add site variable"
    )

    args = parse.parse_args()

    app = yenot.backend.init_application(args.dburl)

    app.add_sitevars(args.sitevar)

    for m in args.module:
        importlib.import_module(m)
    import yenot.backend.api as api

    for func in api.app_init_functions:
        func(app)

    debug = os.environ.get("YENOT_DEBUG", None)
    if debug == "reload":
        kwargs = {"debug": True, "reloader": True}
    elif debug == "debug":
        kwargs = {"debug": True, "reloader": False}
    else:
        kwargs = {}

    import logging
    logging.basicConfig(level=logging.DEBUG)

    # debugging & development service
    app.run(**kwargs)

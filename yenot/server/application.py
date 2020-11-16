import yenot.backend.api as api

app = api.get_global_app()


@app.get("/api/ping", name="ping", skip=["yenot-auth"])
async def ping(request):
    import aiohttp.web as web
    return web.Response(text="Hello, world")
    #return "."


@app.put("/api/request/cancel", name="api_request_cancel")
async def api_request_cancel(request):
    token = request.query.get("token")
    app.cancel_request(token)
    return api.Results().json_out()

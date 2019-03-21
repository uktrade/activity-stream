
async def http_make_request(context, method, url, data, headers):
    async with context.session.request(method, url, data=data, headers=headers) as result:
        # Without this, after some number of requests, they end up hanging
        await result.read()
        return result

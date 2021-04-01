import asyncio

import logging
import os
import sys

from aiohttp import web


async def run_application():
    port = os.environ['PORT']

    async def handle(request):
        fixture = request.match_info['fixture']
        with open(f'./fixture_{fixture}.json', 'rb') as file_obj:
            json_bytes = file_obj.read()
        return web.Response(body=json_bytes)

    app = web.Application()

    app.add_routes([web.get(r'/{fixture:[a-zA-Z]+}', handle)])

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()


def main():
    stdout_handler = logging.StreamHandler(sys.stdout)
    aiohttp_log = logging.getLogger('aiohttp.access')
    aiohttp_log.setLevel(logging.DEBUG)
    aiohttp_log.addHandler(stdout_handler)

    loop = asyncio.get_event_loop()
    loop.create_task(run_application())
    loop.run_forever()


if __name__ == '__main__':
    main()

import asyncio
import logging
import os
import sys
import json

from aiohttp import web
from api_client import APIClient, HawkAuth

LOGGER_NAME = "activity-stream-incoming-feed"


async def run_application():
    app_logger = logging.getLogger(LOGGER_NAME)

    app_logger.debug("Examining environment...")
    port = os.environ["PORT"]
    app_logger.debug("Examining environment: done")

    await create_incoming_application(port)


async def create_incoming_application(port):
    app_logger = logging.getLogger(LOGGER_NAME)

    async def handle_events(request):
        hawk_auth = HawkAuth(
            os.environ["ACTIVITY_STREAM_OUTGOING_ACCESS_KEY_ID"],
            os.environ["ACTIVITY_STREAM_OUTGOING_SECRET_ACCESS_KEY"],
            verify_response=False,
        )
        print(f"hawk_auth: {vars(hawk_auth)}")

        api_client = APIClient(
            api_url=os.environ["ACTIVITY_STREAM_OUTGOING_URL"],
            auth=hawk_auth,
            request=request,
            raise_for_status=False,
            default_timeout=5,
        )
        result = api_client.request(
            request.method,
            "",
            data=json.dumps({"from": 0, "size": 10}),
            headers={
                "Content-Type": request.content_type,
                "X-Forwarded-For": "1.2.3.4,2.3.4.5",
                "X-Forwarded-Proto": "http://localhost:8083",
            },
        )
        print(f"result: {vars(result)}")
        return web.json_response("Hello world")

    app_logger.debug("Creating incoming feed web application...")
    app = web.Application()
    app.add_routes([web.get("/events", handle_events)])

    access_log_format = (
        '%a %t "%r" %s %b "%{Referer}i" "%{User-Agent}i" %{X-Forwarded-For}i'
    )
    runner = web.AppRunner(app, access_log_format=access_log_format)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    app_logger.debug("Creating incoming feed web application: done")


def setup_logging():
    stdout_handler = logging.StreamHandler(sys.stdout)
    aiohttp_log = logging.getLogger("aiohttp.access")
    aiohttp_log.setLevel(logging.INFO)
    aiohttp_log.addHandler(stdout_handler)

    app_logger = logging.getLogger(LOGGER_NAME)
    app_logger.setLevel(logging.DEBUG)
    app_logger.addHandler(stdout_handler)


def main():
    setup_logging()

    loop = asyncio.get_event_loop()
    loop.create_task(run_application())
    loop.run_forever()


if __name__ == "__main__":
    main()

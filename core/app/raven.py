import sentry_sdk
from sentry_sdk.integrations.aiohttp import AioHttpIntegration
from sentry_sdk.integrations.asyncio import AsyncioIntegration


def get_raven_client(sentry, _, __):
    # The older version of the Sentry integration was called "raven" with a client that
    # was passed about. Modern version of the integration doesn expose something to pass
    # about.
    sentry_sdk.init(
        dsn=sentry['dsn'],
        environment=sentry['environment'],
        enable_tracing=True,
        traces_sample_rate=1.0,
        profiles_sample_rate=0.1,
        integrations=[
            AsyncioIntegration(),
            AioHttpIntegration(),
        ],
    )
    return None

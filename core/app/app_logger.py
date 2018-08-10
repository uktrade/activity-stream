import asyncio
import logging

from .app_utils import (
    extract_keys,
)


class ContextAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        return '[%s] %s' % (self.extra['context'], msg), kwargs


def get_logger_with_context(context):
    logger = logging.getLogger('activity-stream')
    return ContextAdapter(logger, {'context': context})


def async_logger(message):
    return async_logger_base(message, lambda _: 'done')


def async_logger_with_result(message):
    return async_logger_base(message, lambda result: result)


def async_logger_base(message, get_success_status):

    def _async_logger(coroutine):
        async def __async_logger(*args, **kwargs):
            kwargs_to_pass, (logger, logger_args,) = extract_keys(
                kwargs,
                ['_async_logger', '_async_logger_args'],
            )

            try:
                logger.debug(message + '...', *logger_args)
                result = await coroutine(*args, **kwargs_to_pass)
                status = get_success_status(result)
                logger_func = logger.debug
                return result
            except asyncio.CancelledError:
                status = 'cancelled'
                logger_func = logger.debug
                raise
            except BaseException:
                status = 'failed'
                logger_func = logger.warning
                raise
            finally:
                logger_func(message + '... (%s)', *(logger_args + [status]))

        return __async_logger

    return _async_logger

import asyncio


def flatten(to_flatten):
    return list(flatten_generator(to_flatten))


def flatten_generator(to_flatten):
    return (
        item
        for sub_list_or_generator in to_flatten
        for item in sub_list_or_generator
    )


async def async_repeat_until_cancelled(context, exception_intervals, coroutine):

    num_exceptions_in_chain = 0

    while True:
        try:
            await coroutine()
            num_exceptions_in_chain = 0
        except asyncio.CancelledError:
            break
        except BaseException:
            interval_index = min(num_exceptions_in_chain, len(exception_intervals) - 1)
            exception_interval = exception_intervals[interval_index]
            num_exceptions_in_chain += 1
            context.logger.exception(
                'Raised exception in async_repeat_until_cancelled. '
                'Waiting %s seconds until looping.', exception_interval)
            context.raven_client.captureException()

            try:
                await asyncio.sleep(exception_interval)
            except asyncio.CancelledError:
                break

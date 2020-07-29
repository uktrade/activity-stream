import asyncio


def flatten(to_flatten):
    return list(flatten_generator(to_flatten))


def flatten_generator(to_flatten):
    return (
        item
        for sub_list_or_generator in to_flatten
        for item in sub_list_or_generator
    )


async def repeat_until_cancelled(context, exception_intervals, to_repeat,
                                 to_repeat_args=(), min_duration=0):
    loop = asyncio.get_running_loop()
    num_exceptions_in_chain = 0

    while True:
        try:
            start = loop.time()
            await to_repeat(*to_repeat_args)
            end = loop.time()
            num_exceptions_in_chain = 0
            await asyncio.sleep(max(min_duration - (end - start), 0))
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

import asyncio
import itertools
import logging
import time


class ExpiringDict:

    def __init__(self, seconds):
        self._seconds = seconds
        self._store = {}

    def _remove_old_keys(self, now):
        self._store = {
            key: (expires, value)
            for key, (expires, value) in self._store.items()
            if expires > now
        }

    def __getitem__(self, key):
        now = int(time.time())
        self._remove_old_keys(now)
        return self._store[key][1]

    def __setitem__(self, key, value):
        now = int(time.time())
        self._remove_old_keys(now)
        self._store[key] = (now + self._seconds, value)

    def __contains__(self, key):
        now = int(time.time())
        self._remove_old_keys(now)
        return key in self._store


class ExpiringSet:

    def __init__(self, seconds):
        self._store = ExpiringDict(seconds)

    def add(self, item):
        self._store[item] = True

    def __contains__(self, item):
        return item in self._store


def flatten(list_to_flatten):
    return [
        item
        for sublist in list_to_flatten
        for item in sublist
    ]


def normalise_environment(key_values):
    ''' Converts denormalised dict of (string -> string) pairs, where the first string
        is treated as a path into a nested list/dictionary structure

        {
            "FOO__1__BAR": "setting-1",
            "FOO__1__BAZ": "setting-2",
            "FOO__2__FOO": "setting-3",
            "FOO__2__BAR": "setting-4",
            "FIZZ": "setting-5",
        }

        to the nested structure that this represents

        {
            "FOO": [{
                "BAR": "setting-1",
                "BAZ": "setting-2",
            }, {
                "BAR": "setting-3",
                "BAZ": "setting-4",
            }],
            "FIZZ": "setting-5",
        }

        If all the keys for that level parse as integers, then it's treated as a list
        with the actual keys only used for sorting

        This function is recursive, but it would be extremely difficult to hit a stack
        limit, and this function would typically by called once at the start of a
        program, so efficiency isn't too much of a concern.
    '''

    # Separator is chosen to
    # - show the structure of variables fairly easily;
    # - avoid problems, since underscores are usual in environment variables
    separator = '__'

    def get_first_component(key):
        return key.split(separator)[0]

    def get_later_components(key):
        return separator.join(key.split(separator)[1:])

    without_more_components = {
        key: value
        for key, value in key_values.items()
        if not get_later_components(key)
    }

    with_more_components = {
        key: value
        for key, value in key_values.items()
        if get_later_components(key)
    }

    def grouped_by_first_component(items):
        def by_first_component(item):
            return get_first_component(item[0])

        # groupby requires the items to be sorted by the grouping key
        return itertools.groupby(
            sorted(items, key=by_first_component),
            by_first_component,
        )

    def items_with_first_component(items, first_component):
        return {
            get_later_components(key): value
            for key, value in items
            if get_first_component(key) == first_component
        }

    nested_structured_dict = {
        **without_more_components, **{
            first_component: normalise_environment(
                items_with_first_component(items, first_component))
            for first_component, items in grouped_by_first_component(with_more_components.items())
        }}

    def all_keys_are_ints():
        def is_int(string):
            try:
                int(string)
                return True
            except ValueError:
                return False

        return all([is_int(key) for key, value in nested_structured_dict.items()])

    def list_sorted_by_int_key():
        return [
            value
            for key, value in sorted(
                nested_structured_dict.items(),
                key=lambda key_value: int(key_value[0])
            )
        ]

    return \
        list_sorted_by_int_key() if all_keys_are_ints() else \
        nested_structured_dict


async def repeat_even_on_exception(never_ending_coroutine, exception_interval, logging_title):
    app_logger = logging.getLogger(__name__)

    while True:
        try:
            await never_ending_coroutine()
        except BaseException as exception:
            app_logger.warning('%s raised exception: %s', logging_title, exception)
        else:
            app_logger.warning(
                '%s finished without exception. '
                'This is not expected: it should run forever.',
                logging_title,
            )
        finally:
            app_logger.warning('Waiting %s seconds until restarting', exception_interval)
            await asyncio.sleep(exception_interval)


def sub_dict_lower(super_dict, keys):
    return {
        key.lower(): super_dict[key]
        for key in keys
    }

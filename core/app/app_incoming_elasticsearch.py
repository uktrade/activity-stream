
def es_search_filtered(permissions, search):
    try:
        query = search['query']
    except KeyError:
        query = {
            'match_all': {},
        }

    bool_query = \
        query if list(query.keys()) == ['bool'] else \
        {'bool': {'must': [query]}}

    bool_filter_maybe_list = bool_query['bool'].get('filter', [])
    bool_filter = \
        bool_filter_maybe_list if isinstance(bool_filter_maybe_list, list) else \
        [bool_filter_maybe_list]

    bool_filter_with_permissions = bool_filter + [
        (
            {'match_all': {}} if perm == '__MATCH_ALL__' else
            {'match_none': {}} if perm == '__MATCH_NONE__' else
            {'terms': {perm['TERMS_KEY']: perm['TERMS_VALUES']}}
        )
        for perm in permissions
    ]

    return {
        'query': {
            'bool': {
                'filter': bool_filter_with_permissions,
                **{key: value for key, value in bool_query['bool'].items() if key != 'filter'},
            },
        },
        **{key: value for key, value in search.items() if key != 'query'},
    }

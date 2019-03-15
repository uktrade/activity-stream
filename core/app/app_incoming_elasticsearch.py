from aiohttp.web import (
    HTTPNotFound,
)
import ujson

from .app_incoming_redis import (
    get_private_scroll_id,
)
from .utils import (
    random_url_safe,
)
from .elasticsearch import (
    ALIAS_ACTIVITIES,
    es_request,
)
from .app_incoming_redis import (
    set_private_scroll_id,
)


async def es_search_query_new_scroll(_, __, query):
    return f'/{ALIAS_ACTIVITIES}/_search', {'scroll': '15s'}, query


async def es_search_query_existing_scroll(context, match_info, _):
    # This is not wrapped in a try/except. This function should only be
    # called if public_scroll_id is in match_info, and there is some server
    # error if this isn't present, and so bubbling up and resulting in a 500
    # is appropriate if a KeyError is thrown
    public_scroll_id = match_info['public_scroll_id']
    private_scroll_id = await get_private_scroll_id(context, public_scroll_id)

    if private_scroll_id is None:
        # It can be argued that this function shouldn't have knowledge that
        # it's called from a HTTP request. However, that _is_ its only use,
        # so KISS, and not introduce more layers unless needed
        raise HTTPNotFound(text='Scroll ID not found.')

    return '/_search/scroll', {'scroll': '30s'}, ujson.dumps({
        'scroll_id': private_scroll_id.decode('utf-8'),
    }, escape_forward_slashes=False, ensure_ascii=False).encode('utf-8')


async def es_search_activities(context, path, query, body, headers, request):

    async def activities(context, elasticsearch_reponse, request):

        async def to_public_scroll_url(context, request, private_scroll_id):
            public_scroll_id = random_url_safe(8)
            await set_private_scroll_id(context, public_scroll_id, private_scroll_id)
            url_with_correct_scheme = request.url.with_scheme(
                request.headers['X-Forwarded-Proto'],
            )
            return str(url_with_correct_scheme.join(
                request.app.router['scroll'].url_for(public_scroll_id=public_scroll_id)
            ))

        elasticsearch_hits = elasticsearch_reponse['hits'].get('hits', [])
        private_scroll_id = elasticsearch_reponse['_scroll_id']
        next_dict = {
            'next': await to_public_scroll_url(context, request, private_scroll_id)
        } if elasticsearch_hits else {}

        return {**{
            '@context': [
                'https://www.w3.org/ns/activitystreams',
                {
                    'dit': 'https://www.trade.gov.uk/ns/activitystreams/v1',
                }
            ],
            'orderedItems': [
                item['_source']
                for item in elasticsearch_hits
            ],
            'type': 'Collection',
        }, **next_dict}

    results = await es_request(
        context=context,
        method='GET',
        path=path,
        query=query,
        headers=headers,
        payload=body,
    )

    response = await results.json()
    return \
        (await activities(context, response, request), 200) if results.status == 200 else \
        (response, results.status)

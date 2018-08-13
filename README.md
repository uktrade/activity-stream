# activity-stream [![CircleCI](https://circleci.com/gh/uktrade/activity-stream.svg?style=svg)](https://circleci.com/gh/uktrade/activity-stream) [![Maintainability](https://api.codeclimate.com/v1/badges/e0284a2cb292704bf53c/maintainability)](https://codeclimate.com/github/uktrade/activity-stream/maintainability) [![Test Coverage](https://api.codeclimate.com/v1/badges/e0284a2cb292704bf53c/test_coverage)](https://codeclimate.com/github/uktrade/activity-stream/test_coverage)

Activity Stream is a collector of various interactions between people and DIT.

## Services data is/will be collected from

- https://opportunities.export.great.gov.uk/
- https://github.com/uktrade/directory-api, which backs the services
  - https://find-a-buyer.export.great.gov.uk/
  - https://selling-online-overseas.export.great.gov.uk/
- https://www.datahub.trade.gov.uk/
- ZendDesk

## Architecture and algorithm

### Full ingest of all activities

A simple paginated HTTP endpoint is exposed in each source service, exposing data in W3C Activity 2.0 format. Concurrently for each source:

- (Delete any unused Elasticsearch indexes for the source)
- A new Elasticsearch index is created with a unique name, and its mappings are set.
- Starting from a pre-configured seed URL:
  - the Activity Stream fetches a page of activities from the URL, and ingests them into the Elasticsearch index;
  - the URL for the next URL is given explicitly in the page;
  - repeat until there is no next URL specified.
- After all pages ingested, the index is aliased to `activities`, with any previous aliases for that source atomically removed.
- Repeat indefinitely.
- On any error, start from the beginning for that source.

This algorithm has a number of nice properties that make it acceptable for an early version.

- Historical activities are ingested with no special import step
- It's self-correcting after downtime of either the source service, or the Activity Stream. No highly available intermediate buffer is required, and the source service does not need to keep track of what was sent to the Activity Stream.
- It's self-correcting in terms of data: historical data can be changed at source, or even deleted, and it will be updated in the Activity Stream in the next pass. It's as GDPR-compliant as the source service is.
- The behaviour of the source service when data is created is in no way dependant on the Activity Stream.
- The Activity Stream can't DoS the source service: as long as each page of data isn't too large, the Activity Stream puts about one extra concurrent user's worth of load on the source service.
- Ingesting all the data repeatedly and indefinitely, as opposed to ingesting on some schedule, means that problems are likely to be found early and often: similar to the principles behind Chaos Engineering/Chaos Monkey.
- Migrations, in terms of updating Elasticsearch mappings, are baked right into the algorithm, and happen on every ingest.
- The fact that it's a single algorithm, with a single low-tech endpoint in each source, that has the above properties. It's one thing to code up, maintain and ensure is secure. This touches production data, and the team involved in this project is small.

### (Almost) real time updates

Real time updates are done using a variation of the algorithm for full ingest. Specifically, after a full ingest has completed:

- The final URL used for the full ingest is saved as the seed for updates.
- Fetch all indexes for each source, both the live one aliased to `activites`, and the one being currently the target of the full ingest.
- Starting from the updates seed URL:
  - the Activity Stream fetches a page of activities from the URL, and ingests them into the Elasticsearch indexes;
  - the URL for the next page is given explicitly in the page;
  - repeat until there is no next URL specified.
- Once all the pages from the updates are ingested, save the final URL used as the seed for the next pass.
- Sleep for 1 second.
- Repeat indefinitely, but if a full ingest has completed, use its final URL as the seed for updates.

A lock is used per feed, that ensures a single in-progress request to each source service. The rest of the behaviour is concurrent.

This algorithm has a number of nice properties

- The Activity Stream doesn't need to be aware of the format of the URLs. It only needs to know the seed URL.
- The source service doesn't need to support multiple methods of fetching data: both real time updates and full ingest uses the same endpoint.
- Once a full ingest is completed, the updates are independant of the time it takes to perform a full ingest.
- Once activities are visible in the `activities` alias, they won't disappear due to race conditions between the full ingest and the updates ingest. This is the reason for ingesting updates into both the live and in-progress indexes.
- This shares a fair bit of code with the full ingest. The team behind this project is small, and so this is less to maintain than other possibilities.
- Similar to the full ingest, the behaviour of the source service when data is created is in no way dependant on the Activity Stream.
- Similar to the full ingest, it doesn't require a highly available intermediate buffer, nor does it require the source service to keep track of what has been sent.
- Similar to the full ingest, it is self-correcting after downtime of either the source service or the Activity Stream.
- Data will still be changed/removed on the next full ingest pass.

In tests, this results in updates being available in Elasticsearch in between 1 and 4 seconds.

## Running tests

Elasticsearch and Redis must be started first, which you can do by

    ./tests_es_start.sh && ./tests_redis_start.sh

and then to run the tests themselves

    ./tests.sh

Higher-level feature tests, that assume very little over the internals of the application, are strongly preferred over lower-level tests that test individual functions, often called unit tests. This involves real Elasticsearch and Redis, and firing up various HTTP servers inside the test environment: since this is how the application interacts with the world in production. Mocking/patching is done for speed or determinism reasons: e.g. patching `asyncio.sleep` or `os.urandom`.

## Outgoing and incoming applications

The core of the Activity Stream is made of two applications.

### Outgoing

This is the application that performs the above algorithm, continually making <em>outgoing</em> HTTP connections to pull data. It is not scalable, and other than during deployments, there should only be one instance running at any time. However, it has low CPU requirements, and as explained above, self-correcting after any downtime.

### Incoming

This is the application that features a HTTP server, accepting <em>incoming</em> HTTP requests, and passes requests for data to Elasticsearch. It converts the raw Elasticsearch format returned into a Activity Streams 2.0 compatible format. This is scalable, and multiple instances of this application can be running at any given time.

## Elasticsearch / Kibana proxy

A proxy is provided to allow developer access to Elasticsearch / Kibana in [elasticsearch_proxy](elasticsearch_proxy).

## Logging

Logs are usually output in the format

    [application name,optional comma separated contexts] Action... (completion status)

For example two message involved in gathering metrics would be shown in Kibana as

    August 12th 2018, 10:59:45.829  [outgoing,metrics] Saving to Redis... (done)
    August 12th 2018, 10:59:45.827  [outgoing,metrics] Saving to Redis...

There are potentially many separate chains of concurrent behaviour at any given time: the context is used to help quickly distinguish them in the logs. For the outpoing application when it fetches data, the context is a human-readable unique identifier for the source. For incoming requests, the context is a unique identifier generated at the beginning of the request.

    [elasticsearch-proxy,IbPY5ozS] Elasticsearch request by (ACCOUNT_ID) to (GET) (dev-v6qrmm4neh44yfdng3dlj24umu.eu-west-1.es.amazonaws.com:443/_plugin/kibana/bundles/one)...
    [elasticsearch-proxy,IbPY5ozS] Receiving request (10.0.0.456) (GET /_plugin/kibana/bundles/commons.style.css?v=16602 HTTP/1.1) (Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36) (1.2.3.4, 127.0.0.1)

## Verification Feed

A small separate application in [verification_feed](verification_feed) is provided to allow the stream to be tested, even in production, without using real data. It provides a number of activities, published date of the moment the feed is queried.

## Running locally

Since the tests are fairly high level, most development should be able to be done without starting the application separately. However, if you do wish to run the application locally, you must have a number of environment variables set: the up-to-date list of these are in the `mock_env` function defined in [tests_utils.py](core/tests_utils.py). Then to run the application that polls feeds

    (cp -r -f shared core && cd core && python -m app.app_outgoing)

or to run the application that proxies incoming requests to Elasticsearch

    (cp -r -f shared core && cd core && python -m app.app_incoming)

This closely resembles how the CI pipeline builds and deploys the applications.

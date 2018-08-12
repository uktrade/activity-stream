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

A simple paginated HTTP endpoint is exposed in each source service, exposing data in W3C Activity 2.0 format. For each source:

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

Of course, it is _not_ optimised for real time updates. Updates for new data will appear after the next full ingest.

## Running tests

Elasticsearch and Redis must be started first, which you can do by

    ./tests_es_start.sh && ./tests_redis_start.sh

and then to run the tests themselves

    ./tests.sh

## Elasticsearch / Kibana Proxy

A proxy is provided to allow developer access to Elasticsearch / Kibana in [elasticsearch_proxy](elasticsearch_proxy).

## Verification Feed

A small separate application in [verification_feed](verification_feed) is provided to allow the stream to be tested, even in production, without using real data. It provides a number of activities, published date of the moment the feed is queried.

## Running locally

The tests are fairly high level, and most development should be able to be done without starting the application separately. However, if you do wish to run the application locally, you must have a number of environment variables set: the up-to-date list of these are in the `mock_env` function defined in [tests_utils.py](core/tests_utils.py). Then to run the application that polls feeds

    (cp -r -f shared core && cd core && python -m app.app_outgoing)

or to run the application that proxies incoming requests to Elasticsearch

    (cp -r -f shared core && cd core && python -m app.app_incoming)

This closely resembles how the CI pipeline builds and deploys the applications.

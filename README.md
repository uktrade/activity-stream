# activity-stream [![CircleCI](https://circleci.com/gh/uktrade/activity-stream.svg?style=svg)](https://circleci.com/gh/uktrade/activity-stream) [![Maintainability](https://api.codeclimate.com/v1/badges/e0284a2cb292704bf53c/maintainability)](https://codeclimate.com/github/uktrade/activity-stream/maintainability) [![Test Coverage](https://api.codeclimate.com/v1/badges/e0284a2cb292704bf53c/test_coverage)](https://codeclimate.com/github/uktrade/activity-stream/test_coverage)

Activity Stream is a collector of various interactions between people and DIT.

## Services data is/will be collected from

- https://opportunities.export.great.gov.uk/
- https://github.com/uktrade/directory-api, which backs the services
  - https://find-a-buyer.export.great.gov.uk/
  - https://selling-online-overseas.export.great.gov.uk/
- https://www.datahub.trade.gov.uk/
- ZendDesk

## Running tests

Elasticsearch and Redis must be started first, which you can do by

    ./tests_es_start.sh && ./tests_redis_start.sh

and then to run the tests themselves

    ./tests.sh

## Verification Feed

A small separate application in [verification_feed](verification_feed) is provided to allow the stream to be tested, even in production, without using real data. It provides a single activity, published date of the moment the feed is queried.

## Elasticsearch / Kibana Proxy

A proxy is provided to allow developer access to Elasticsearch / Kibana in [elasticsearch_proxy](elasticsearch_proxy).

## Running locally

The tests are fairly high level, and most development should be able to be done without starting the application separately. However, if you do wish to run the application locally, you must have a number of environment variables set: the up-to-date list of these are in the `mock_env` function defined in [tests_utils.py](core/tests_utils.py). Then to run the application that polls feeds

    (cp -r -f shared core && cd core && python -m app.app_outgoing)

or to run the application that proxies incoming requests to Elasticsearch

    (cp -r -f shared core && cd core && python -m app.app_incoming)

This closely resembles how the CI pipeline builds and deploys the applications.

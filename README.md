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

    ./tests.sh

## Running locally

    PORT=8080 \
    FEEDS__1__SEED=http://some-endpoint/ \
    FEEDS__1__ACCESS_KEY_ID=feed-some-id \
    FEEDS__1__SECRET_ACCESS_KEY=feed-some-secret \
    FEEDS__1__TYPE=elasticsearch_bulk \
    ELASTICSEARCH__AWS_ACCESS_KEY_ID=some-id \
    ELASTICSEARCH__AWS_SECRET_ACCESS_KEY=aws-secret \
    ELASTICSEARCH__HOST=127.0.0.1 \
    ELASTICSEARCH__PORT=8082 \
    ELASTICSEARCH__PROTOCOL=http \
    ELASTICSEARCH__REGION=us-east-2 \
    INCOMING_ACCESS_KEY_PAIRS__1__KEY_ID=some-id \
    INCOMING_ACCESS_KEY_PAIRS__1__SECRET_KEY=some-secret \
    INCOMING_IP_WHITELIST=1.2.3.4 \
    python -m core.app

## Managing Requirements

When adding a new library, first add it to requirements.in, then::

    pip install pip-tools
    pip-compile --output-file requirements.txt requirements.in
    pip install -r requirements.txt

## Endpoints

The server responds with a 200 to GET /, and it returns an error code otherwise.

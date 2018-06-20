# activity-stream [![CircleCI](https://circleci.com/gh/uktrade/activity-stream.svg?style=svg)](https://circleci.com/gh/uktrade/activity-stream) [![Maintainability](https://api.codeclimate.com/v1/badges/e0284a2cb292704bf53c/maintainability)](https://codeclimate.com/github/uktrade/activity-stream/maintainability)

Activity Stream is a collector of various interactions between people and DIT.

## Services data is/will be collected from

- https://opportunities.export.great.gov.uk/
- https://find-a-buyer.export.great.gov.uk/
- https://selling-online-overseas.export.great.gov.uk/
- https://www.datahub.trade.gov.uk/
- CMS Contact Form

## Running tests

    ./tests.sh

## Running locally

    PORT=8080 \
    FEED_ENDPOINT=http://some-endpoint/ \
    FEED_ACCESS_KEY_ID=feed-some-id \
    FEED_SECRET_ACCESS_KEY=feed-some-secret \
    ELASTICSEARCH_AWS_ACCESS_KEY_ID=some-id \
    ELASTICSEARCH_AWS_SECRET_ACCESS_KEY=aws-secret \
    ELASTICSEARCH_HOST=127.0.0.1 \
    ELASTICSEARCH_PORT=8082 \
    ELASTICSEARCH_PROTOCOL=http \
    ELASTICSEARCH_REGION=us-east-2 \
    python -m core.app

## Managing Requirements

When adding a new library, first add it to requirements.in, then::

    pip install pip-tools
    pip-compile --output-file requirements.txt requirements.in
    pip install -r requirements.txt

## Endpoints

The server responds with a 200 to GET /, and it returns an error code otherwise.

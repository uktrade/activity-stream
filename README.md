# activity-stream [![CircleCI](https://circleci.com/gh/uktrade/activity-stream.svg?style=svg)](https://circleci.com/gh/uktrade/activity-stream)

Activity Stream is a collector of various interactions between people and DIT.

## Running tests

    ./tests.sh

## Running locally

    FEED_ENDPOINT=http://some-endpoint/ \
    INTERNAL_API_SHARED_SECRET=some-secret \
    ELASTIC_SEARCH_ENDPOINT=https://some-elastic-search-endpoint/ \
    python -m core.app

## Managing Requirements

When adding a new library, first add it to requirements.in, then::

    pip install pip-tools
    pip-compile --output-file requirements.txt requirements.in
    pip install -r requirements.txt

## Endpoints

The server responds with a 200 to GET /, and it returns an error code otherwise.

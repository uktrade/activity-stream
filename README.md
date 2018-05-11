# activity-stream [![CircleCI](https://circleci.com/gh/uktrade/activity-stream.svg?style=svg)](https://circleci.com/gh/uktrade/activity-stream)

Activity Stream is a collector of various interactions between people and DIT.

## Running tests

    ./tests.sh

## Running locally

    gunicorn core.app --config conf/gunicorn.py

## Managing Requirements

When adding a new library, first add it to requirements.in, then::

    pip install pip-tools
    pip-compile --output-file requirements.txt requirements.in
    pip install -r requirements.txt

## Endpoints

The server responds with a 200 to all HTTP requests.

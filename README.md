# activity-stream [![CircleCI](https://circleci.com/gh/uktrade/activity-stream.svg?style=svg)](https://circleci.com/gh/uktrade/activity-stream) [![Maintainability](https://api.codeclimate.com/v1/badges/e0284a2cb292704bf53c/maintainability)](https://codeclimate.com/github/uktrade/activity-stream/maintainability) [![Test Coverage](https://api.codeclimate.com/v1/badges/e0284a2cb292704bf53c/test_coverage)](https://codeclimate.com/github/uktrade/activity-stream/test_coverage)

Activity Stream pulls a selection of data from several DIT services into one ElasticsSearch Database. This data is searchable through an API and is connected to Datahub.

Data is collected from:

- Opportunities and Enquiries from the Export Opportunities project https://opportunities.export.great.gov.uk/
- Articles from the Directory CMS project https://github.com/uktrade/directory-api, which backs the services
  - https://find-a-buyer.export.great.gov.uk/-
- Marketplaces from the Selling Online Overseas Project https://selling-online-overseas.export.great.gov.uk/
- Events from the Events API
- ZendDesk Tickets
- https://www.datahub.trade.gov.uk/
- Specific ad-hoc web pages added to search at https://www.great.gov.uk/search-key-pages

# Installation Instructions

    $ git clone https://github.com/uktrade/great-domestic-ui
    $ cd great-domestic-ui

Install [Virtual Env](https://virtualenv.pypa.io) if you haven't got it installed locally, then create one using:

    $ virtualenv .venv -p python3.6

Next follow the instructions in the section below below to automatically set environment variables when this starts up.

Finally, start the virtual environment with

    $ source .venv/bin/activate

Install the requirements

    $ pip install -r core/requirements.txt
    $ pip install -r core/requirements_test.txt

Install the automatic linter, Pre-commit

    $ pip install pre-commit

## Adding the Env Vars to VirtualEnv setup

This is one approach to adding environment variables automatically.

Add to the bottom of `ENV/bin/activate`

```# set environment variables - any added here should be "unset" list at top

export PORT=8080
export FEEDS__1__UNIQUE_ID=verification_feed_app
export FEEDS__1__SEED=http://localhost:8082/0
export FEEDS__1__ACCESS_KEY_ID=
export FEEDS__1__SECRET_ACCESS_KEY=
export FEEDS__1__TYPE=activity_stream
export INCOMING_ACCESS_KEY_PAIRS__1__KEY_ID=incoming-some-id-1
export INCOMING_ACCESS_KEY_PAIRS__1__SECRET_KEY=incoming-some-secret-1
export INCOMING_ACCESS_KEY_PAIRS__1__PERMISSIONS__1=POST
export INCOMING_ACCESS_KEY_PAIRS__2__KEY_ID=incoming-some-id-2
export INCOMING_ACCESS_KEY_PAIRS__2__SECRET_KEY=incoming-some-secret-2
export INCOMING_ACCESS_KEY_PAIRS__2__PERMISSIONS__1=POST
export INCOMING_ACCESS_KEY_PAIRS__3__KEY_ID=incoming-some-id-3
export INCOMING_ACCESS_KEY_PAIRS__3__SECRET_KEY=incoming-some-secret-3
export INCOMING_ACCESS_KEY_PAIRS__3__PERMISSIONS__1=GET
export INCOMING_IP_WHITELIST__1=1.2.3.4
export INCOMING_IP_WHITELIST__2=2.3.4.5
export SENTRY_DSN=http://abc:cvb@localhost:9872/123
export SENTRY_ENVIRONMENT=test
export VCAP_SERVICES='{"redis":[{"credentials":{"uri":"redis://127.0.0.1:6379"}}],"elasticsearch":[{"credentials":{"uri":"http://some-id:some-secret@127.0.0.1:9200"}}]}'
```

And add to the bottom of the `deactivate ()` function in that file. 

```
    # unset environment variables - this must match "unset" list at top
    unset PORT
    unset FEEDS__1__UNIQUE_ID
    unset FEEDS__1__SEED
    unset FEEDS__1__ACCESS_KEY_ID
    unset FEEDS__1__SECRET_ACCESS_KEY
    unset FEEDS__1__TYPE
    unset FEEDS__2__UNIQUE_ID
    unset FEEDS__2__SEED
    unset FEEDS__2__ACCESS_KEY_ID
    unset FEEDS__2__SECRET_ACCESS_KEY
    unset FEEDS__2__TYPE
    unset FEEDS__3__UNIQUE_ID
    unset FEEDS__3__SEED
    unset FEEDS__3__ACCESS_KEY_ID
    unset FEEDS__3__SECRET_ACCESS_KEY
    unset FEEDS__3__TYPE
    unset INCOMING_ACCESS_KEY_PAIRS__1__KEY_ID
    unset INCOMING_ACCESS_KEY_PAIRS__1__SECRET_KEY
    unset INCOMING_ACCESS_KEY_PAIRS__1__PERMISSIONS__1
    unset INCOMING_ACCESS_KEY_PAIRS__2__KEY_ID
    unset INCOMING_ACCESS_KEY_PAIRS__2__SECRET_KEY
    unset INCOMING_ACCESS_KEY_PAIRS__2__PERMISSIONS__1
    unset INCOMING_ACCESS_KEY_PAIRS__3__KEY_ID
    unset INCOMING_ACCESS_KEY_PAIRS__3__SECRET_KEY
    unset INCOMING_ACCESS_KEY_PAIRS__3__PERMISSIONS__1
    unset INCOMING_IP_WHITELIST__1
    unset INCOMING_IP_WHITELIST__2
    unset SENTRY_DSN
    unset SENTRY_ENVIRONMENT
    unset VCAP_SERVICES
```

# Running the apps locally

Run Elasticsearch and Redis using docker

    ./tests_es_start.sh && ./tests_redis_start.sh

Running the API server a.k.a. "Incoming":

    python3 -m core.app.app_incoming

Running the data collection script a.k.a  "Outgoing":

    python3 -m core.app.app_outgoing

Running an example app that Outgoing will pull data from:

    PORT=8082 python3 verification_feed/app.py


# Running the tests

Ensure dependencies are installed and Elasticsearch and Redis are running

    $ install -r core/requirements_test.txt
    $ ./tests_es_start.sh && ./tests_redis_start.sh

To run all of the tests

    ./tests.sh

(Or directly using minitest)

    python3 -m unittest 

Running a single test takes the format:

    python3 -m unittest -v core.tests.tests.TestApplication.test_aventri

## Release Process

### Linting

A tool called [Pre-commit](https://pre-commit.com/) must be installed. This will run linting when committing.

### Commit Signing

Commits must be signed to be submitted to github. Use [GPG Signer](https://git-scm.com/book/en/v2/Git-Tools-Signing-Your-Work)

### Deployment

Deploy master branch or feature branches to development, staging or production here:

https://jenkins.ci.uktrade.io/job/activity-stream/

## Dashboards and Metrics

Due to the quantity of requests, the logs can be difficult to interpret. Dashboards are used to identify issues early.

Production Dashboard: https://grafana.ci.uktrade.io/d/gKcrzUKmz/activity-stream?orgId=1&var-instance=activity-stream.london.cloudapps.digital:443

Staging Dashboard: https://grafana.ci.uktrade.io/d/gKcrzUKmz/activity-stream?orgId=1&var-instance=activity-stream-staging.london.cloudapps.digital:443

Explanation of graphs:

*Feed failures in previous minute*
Shows whether feeds are connected and active correctly, and should show zero for all feeds.

*Age of youngest activity*
Shows the age of the most recent activity, and should be flat. If the graph is going up over time, no activities are being consumed and this indicates that the "outgoing" script is down.

## Logging

Logs are of the format

    Date [application, tags] Action... (status)

For example

    August 12th 2018, 10:59:45.827  [outgoing,metrics] Saving to Redis...
    August 12th 2018, 10:59:45.829  [outgoing,metrics] Saving to Redis... (done)

Incoming requests are tagged with a unique identifier. For example:

    [elasticsearch-proxy,IbPY5ozS] Elasticsearch request by (ACCOUNT_ID) to (GET) (dev-v6qrmm4neh44yfdng3dlj24umu.eu-west-1.es.amazonaws.com:443/_plugin/kibana/bundles/one)...
    [elasticsearch-proxy,IbPY5ozS] Receiving request (10.0.0.456) (GET /_plugin/kibana/bundles/commons.style.css?v=16602 HTTP/1.1) (Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36) (1.2.3.4, 127.0.0.1)

To view the logs

cf login
-> dit-services (for prod) OR dit-staging (for dev & staging)
-> activity-stream OR activity-stream-staging OR activity-stream-dev
cf logs [app_name]

To isolate a particular stream of logs use `| grep` and the app or tag name

    cf logs activity-stream-staging | grep incoming
    cf logs activity-stream-staging | grep selling_online_overseas_markets

# How ActivityStream Works

Activity Stream has two main applications. A script called "Outgoing" collects data from each project and saves it in elasticsearch. An API called "Incoming" provides access to this data.

## The "Outgoing" Script

Data from each endpoint are saved in seperate indices in Elasticsearch. Each item is saved twice, first in one index for Activities and one for Objects. The "Activities" index stores each action performed on the object ("Created, Updated") as a different item and so a given object can appear multiple times. The "Objects" index for a particular data type only stores the most recent version of a given object - each item of data is unique.

Each project connected to Activity Stream has an API that the Activity Stream "Outgoing" script can crawl. For consistency the data should be presented in W3C Activity 2.0 format. Specifically, it must offer a `Collection` with the activites in the `orderedItems` key. The API in each project should provide the data as a list of all data, ordered by some attribute (e.g. date updated), one page at a time. Each "page" of data also provides a link to the next page in the chain in the key `next`, until the last page which does not have a `next` key.

As it consumes an API, the Outgoing Script will create a new pair of temporary indices (one for activities and one for objects) for each source. When the script has consumed the last page of data, the current pair of indexes are deleted and replaced by the just-created temporary indices, and the process immediately begins again from the first page. The script is not polling the source on a scheduled basis, but rather continuously and repeatedly downloading the data.

Repeatedly downloading the data keeps the data in Activity Stream up to date. If there are any errors in the script, it restarts. Because Activity Stream deletes any data shortly after the source does, Activity Stream is also GDPR compliant. Outgoing is self-correcting after any downtime of either Activity Stream or the source.

Outgoing does not know about the format of the data that is consumed or provided. When the format of the data changes, "Outgoing" automatically deletes the old data and replaces it with the new format data after one cycle.

Outgoing uses Hawk Authentication; a valid Hawk protocal `Authorization` header must be provided. The endpoint queried must also be included as an item in the IP_WHITELIST

Outgoing is not scalable, besides during deployments there should only be one instance running.

Each activity must have an `id` field that is unique. In the current implementation, it doesn't have to be unique accross all sources, but it is planned to in future. It is recommend to ensure this is unique accross all sources.

### Performance requirements of source APIs

The Outgoing script only makes a single connection to the source service at any given time, and will not follow the next page of pagination until the previous one is retrieved. It continuously puts one extra user's worth of load on the source service. The source APIs should be able to handle concurrent users to support this.

The extra user performs a database query multiple times every second. For this reason it's important to optimise the performance of the source APIs. For example, paginating using a simple offset/limit in a SQL query is extremely inefficient on later pages. Pagination based on a combination of create/update timestamp and primary key, together with appropriate indexes in the database, works well. See one of the existing projects for an example of this.

It is possible to rate limit the Activity Stream if necessary. The source API should respond with HTTP 429, containing a `Retry-After` header containing how after many seconds the Activity Stream should retry the same URL.

## The "Incoming" API

The Inbound API provides two endpoints:

    /v1/activities

For consuming all data stored in the activities indices that meet a given elasticsearch query. Results are returned one page at a time.

    /v1/objects

Is used by Great.gov.uk seach, provides data stored in the objects indices that meet a given elasticsearch query.

The paginated feed can output the same activity multiple times, and as long as each has the same `id`, it won't be repeated in Elasticsearch.


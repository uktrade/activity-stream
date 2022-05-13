# activity-stream [![CircleCI](https://circleci.com/gh/uktrade/activity-stream.svg?style=shield)](https://circleci.com/gh/uktrade/activity-stream) [![Maintainability](https://api.codeclimate.com/v1/badges/6f06e9a01e1ad350286e/maintainability)](https://codeclimate.com/github/uktrade/activity-stream/maintainability) [![Test Coverage](https://api.codeclimate.com/v1/badges/6f06e9a01e1ad350286e/test_coverage)](https://codeclimate.com/github/uktrade/activity-stream/test_coverage)

Activity Stream pulls a selection of data from several DIT & external services into one Elasticsearch Database. This data is searchable through an API and is connected to Datahub. There is very little business logic in terms of user-facing features: it takes data from one place to another.

Data is collected from:

- Opportunities and Enquiries from the Export Opportunities project https://opportunities.export.great.gov.uk/
- Articles from the Directory CMS project https://github.com/uktrade/directory-api, which backs the services
  - https://find-a-buyer.export.great.gov.uk/-
- Marketplaces from the Selling Online Overseas Project https://selling-online-overseas.export.great.gov.uk/
- Events from the Events API
- ZendDesk Tickets
- https://www.datahub.trade.gov.uk/
- Specific ad-hoc web pages added to search at https://www.great.gov.uk/search-key-pages

# Installation

    $ git clone https://github.com/uktrade/activity-stream
    $ cd activity-stream

Install [Virtualenv](https://virtualenv.pypa.io) if you haven't got it installed locally, then create one using:

    $ virtualenv .venv -p python3.6

You will need environment variables activated to run the main scripts. One time saving approach is to automatically set the environment variables when you start your virtual environment, one approach to which is described in the section below.

Finally, start the virtual environment with

    $ source .venv/bin/activate

Install the requirements

    $ pip install -r core/requirements.txt
    $ pip install -r core/requirements_test.txt

Install the automatic linter, Pre-commit

    $ pip install pre-commit
    $ pre-commit install

## Adding the environment variables to Virtualenv setup

This is one approach to adding environment variables automatically each time you start your virtual environment.

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
    # unset environment variables
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

## Populating an empty local database with data

The database ActivityStream is in a docker image and it is wiped of data each time you restart the image.

Either:

1. Start the sample project ("Verification Feed") using the instuctions above, and then start the Outgoing script. You have already set up the Outgoing script with environment variables to scrape the Verification Feed.

Or:

2. Add a new set of environment variables that point the locally running Outgoing script to the currently running endpoints for one of the projects in staging. A few sets of environment variables to add for various feeds can be found in Vault in the "Activity Stream > Staging" folder. When these have been added, run the Outgoing script. Optionally add these environment variables to your `ENV/bin/activate` script to have them run each time

## Viewing your local data

To quickly check how many documents are in your activity stream database and the current names of the indices, use:

    $ curl -X GET "localhost:9200/_cat/indices?v"

To look at the data themselves, download Kibana 6.3.0 [here](https://www.elastic.co/downloads/past-releases/kibana-6-3-0). Follow the instructions for running the Kibana server. When running, you will need to configure Kibana to read from the indices. Direct your browser to localhost:5601 and in the "management" section, add the following patterns: "objects*" and "activities*". You will now be able to select these patterns in the "visualise" section and see your data.



# Running the tests

Ensure dependencies are installed and Elasticsearch and Redis are running

    $ pip install -r core/requirements_test.txt
    $ ./tests_es_start.sh && ./tests_redis_start.sh

To run all of the tests

    ./tests.sh

    python3 -m unittest

Running a single test takes the format:

    ./tests.sh core.tests.tests.TestApplication.test_aventri

## Release process

### Linting

A tool called [Pre-commit](https://pre-commit.com/) must be installed. This will run linting when committing.

### Deployment

Deploy master branch or feature branches to development, staging or production using Jenkins.

## Dashboards and metrics

Due to the quantity of requests, the logs can be difficult to interpret. Each task outputs Activity Stream-specific metrics that allows each to be analysed separately. Dashboards are used to identify issues early. The grafana URLs are available from any developer associated with the project.

Explanation of graphs:

*Feed failures in previous minute*
Shows whether feeds are connected and active correctly, and should mostly show zero for all feeds.

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

Activity Stream has two main applications. A script called "Outgoing" collects data from each project and saves it in Elasticsearch. An API called "Incoming" provides access to this data.

## The "Outgoing" Script

Data from each endpoint are saved in seperate indices in Elasticsearch. Each item is saved twice, first in one index for Activities and one for Objects. The "Activities" index stores each action performed on the object ("Created, Updated") as a different item and so a given object can appear multiple times. The "Objects" index for a particular data type only stores the most recent version of a given object - each item of data is unique.

Each project connected to Activity Stream has an API that the Activity Stream "Outgoing" script can crawl. For consistency the data should be presented in W3C Activity 2.0 format. Specifically, it must offer a `Collection` with the activites in the `orderedItems` key. The API in each project should provide the data as a list of all data, which must be ordered by date updated ascending, one page at a time. Each "page" of data also provides a link to the next page in the chain in the key `next`, until the last page which does not have a `next` key.

As it consumes an API, the Outgoing Script will create a new pair of temporary indices (one for activities and one for objects) for each source. When the script has consumed the last page of data, the current pair of indexes are deleted and replaced by the just-created temporary indices, and the process immediately begins again from the first page. The script is not polling the source on a scheduled basis, but rather continuously and repeatedly downloading the data. This process is called the "full ingest cycle"

The Outgoing Script also has a second process that repeatedly polls the last page (i.e. most recently updated data) for new data. This is called the "update cycle". This updated data are added to both the currently active indices for that feed (both Activities and Objects) and also the indices being currently constructed by the in-progress full ingest cycle.

Repeatedly downloading the data keep the data in Activity Stream up to date. If there are any errors during ingesting one of the feeds into Elasticsearch, that particular feed restarts. Outgoing is self-correcting after any downtime of either Activity Stream or the source. As Activity Stream deletes any data shortly after the source does, Activity Stream is as GDPR compliant as the sources it pulls from.

Outgoing does not know about the format of the data that is consumed or provided. When the format of the data changes, "Outgoing" automatically deletes the old data and replaces it with the new format data after one cycle.

Outgoing is not scalable, besides during deployments there should only be one instance running.

### Performance requirements of source APIs

The Outgoing script only makes a single connection to the source service at any given time, and will not follow the next page of pagination until the previous one is retrieved. It continuously puts one extra user's worth of load on the source service. The source APIs should be able to handle concurrent users to support this.

The extra user performs a database query multiple times every second. For this reason it's important to optimise the performance of the source APIs. For example, paginating using a simple offset/limit in a SQL query is extremely inefficient on later pages. Pagination based on a combination of create/update timestamp and primary key, together with appropriate indexes in the database, works well. See one of the existing projects for an example of this.

It is possible to rate limit the Activity Stream if necessary. The source API should respond with HTTP 429, containing a `Retry-After` header containing how after many seconds the Activity Stream should retry the same URL.

## The "Incoming" API

The Inbound API provides two endpoints:

    /v1/activities

For consuming all data stored in the activities indices that meet a given Elasticsearch query. Results are returned one page at a time.

    /v1/objects

Is used by great.gov.uk seach, provides data stored in the objects indices that meet a given Elasticsearch query.

The paginated feed can output the same activity multiple times, and as long as each has the same `id`, it won't be repeated in Elasticsearch.

The Incoming API uses Hawk Authentication; a valid Hawk protocol `Authorization` header must be provided on the inbound request. The request must also included an 'X-Forwarded-For' header (automatically added by GOV.UK PaaS) including an IP in the IP_WHITELIST, therefore the IP_WHITELIST environment variables must be kept up to date with changes to IPs in Gov PaaS.

# Notes on approach to development

The outgoing application has a number of properties/requirements that are different to a standard web-based application. Below are the properties, followed by the development patterns used to support this.

- Chains of operations that effect data changes can be long running. A full ingest can take hours: there is no "instantaenous" web request to do this. (There are quicker changes: "almost" real time updates that take between 1 and 4 seconds.)

- It's extremely asynchronous in terms of cause and effect, in that a change on a source feed only makes a difference to the data exposed by the AS "some time later". And this time can be different: an update can be quick, but a deletion would be on the next full ingest. (This is by design. Even if a source feed attempted to "push" data synchronously, it would need to handle failure, which would have to be asynchronous. Instead of having this asynchronous behaviour in all sources, it's here.)

- It's multi-tasked (the asyncio version of multi-threaded), with communication between tasks. At the time of writing, production has [at least] 17 tasks.

- It depends heavily on properties of Elasticsearch that other data stores don't have

- It depends heavily on the sequence of the calls to Elasticsearch e.g. creating, deleting and refreshing indexes, modifying aliases.

- There is no interface to load up manually and check that "it all still works".

As such, the code has the below properties / follows the below patterns.

- It's light on abstraction: to make changes you need to understand the "guts", so they are not hidden. For example, abstracting Elasticsearch [probably] won't give value, because it would either have to expose everything underneath, or to make changes you would have to change what's hidden.

- But there is some abstraction: source feeds can be of different types, with slightly different data formats or authentication patterns. This is abstracted so the rest of the algorithm "doesn't have to care" about these things.

- There isn't much object-oriented code, since most concerns are cross-cutting, in that there are queries to source feeds, Elasticsearch, logging, metric, and Redis code.

- Mutation is minimised wherever possible. There is a lot of state by nature of the algorithm, so it's more important to not make this "even worse" so the possibiliy of bugs caused by unexpected mutation(/lack of it) is reduced.

- Logging requires an amount of "injection": each task is logged so each can be grepped for in output separately. The same function can require different logging output depending on which task its called from, and so needs a logging _context_ made available to it, which is done by passing it as an explicit function argument. An alternative is to use decorators/similar, but because these would have to be dynamic, this would be more complicated since it would use more functions that return functions.

  Metrics are calculated "per feed", and so related structures are passed into functions for similar reasons as logging.

- Dependencies are usually "injected" into functions as manual function arguments [often inside the `context` variable].

  - To aid explicitness: the "algorithmic", long-running and asynchronous nature of the problem, means there are a lot of implicit requirements on each line of code on what came before, potentially hours ago. It's therefore more important to not make this "even worse".

  - This then helps for the future. Refactoring and changes are, usually, safer if for every function everything it depends on is explicit in its argument list, and its effect is by its return value [or what it does to things in its argument list]. This can be "more to take in" at first glance, but it's things that are important to understand _anyway_.

    Put another way, this project is complex due to unavoidable dependencies between bits of code; all thing being equal, it's better to deal with the complexity up-front than after a bug has been introduced into production.

- Tests are "end to end": it would be error-prone and time consuming to manually go through the many important asynchronous edge cases, such as the "eventual" recovery after a failure of a source feed or Elasticsearch.

  The tests allow refactoring, and are correctly sensitive to broken behaviour. However, there are downsides. They often offer little information on what is broken. This is compounded by the self-correcting nature of the code: it attempts to recover after exceptions, and so the tests may just sit there. Another downside is that each test requires a lot of setup: source feeds, Elasticsearch, Redis, and uses a HTTP client to query for data that often has to poll until a condition is met.

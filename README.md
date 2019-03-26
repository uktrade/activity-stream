# activity-stream [![CircleCI](https://circleci.com/gh/uktrade/activity-stream.svg?style=svg)](https://circleci.com/gh/uktrade/activity-stream) [![Maintainability](https://api.codeclimate.com/v1/badges/e0284a2cb292704bf53c/maintainability)](https://codeclimate.com/github/uktrade/activity-stream/maintainability) [![Test Coverage](https://api.codeclimate.com/v1/badges/e0284a2cb292704bf53c/test_coverage)](https://codeclimate.com/github/uktrade/activity-stream/test_coverage)

Activity Stream pulls all available data from several DIT services into one ElasticsSearch Database. This data is searchable via an API and is connected to Datahub.

Data is collected from:

- Opportunities and Enquiries from the Export Opportunities project https://opportunities.export.great.gov.uk/
- Articles from the Directory CMS project https://github.com/uktrade/directory-api, which backs the services
  - https://find-a-buyer.export.great.gov.uk/-
- Marketplaces from the Selling Online Overseas Project https://selling-online-overseas.export.great.gov.uk/
- Events from the Events API
- ZendDesk Tickets
- https://www.datahub.trade.gov.uk/

# Installation Instructions

Clone this repository
Set up Virtual Env
Start Virtual Env
Add the local environment variables to Virtual Env

```bash
pip install -r core/requirements.txt
```


# Running the apps locally

A small separate source API application in [verification_feed](verification_feed) is provided to allow the stream to be tested. It provides a small number of activities.

Add env variables:
export FEEDS__2__UNIQUE_ID=verification_feed
export FEEDS__2__SEED=http://localhost:8082/0
export FEEDS__2__ACCESS_KEY_ID=feed-some-id
export FEEDS__2__SECRET_ACCESS_KEY='?[!@$%^%'
export FEEDS__2__TYPE=activity_stream

Run app on
`PORT=8082 python3 app.py`

CHANGE:
and have a number of environment variables set: the up-to-date list of these are in the `mock_env` function defined in [tests_utils.py](core/tests_utils.py). Then to run the application that polls feeds

    (cp -r -f shared core && cd core && python3 -m app.app_outgoing)

or to run the application that proxies incoming requests to Elasticsearch

    (cp -r -f shared core && cd core && python3 -m app.app_incoming)


Run Elasticsearch and Redis using docker

    ./tests_es_start.sh && ./tests_redis_start.sh

and then to run the



# Running the tests

You must have the dependencies specified in [core/requirements_test.txt](core/requirements_test.txt), which can be installed by

```bash
pip install -r core/requirements_test.txt
```

Ensure Elasticsearch and Redis are running using docker

    ./tests_es_start.sh && ./tests_redis_start.sh

and then to run the tests themselves

    ./tests.sh

Individual Tests

Running a single test takes the format:
`python3 -m unittest -v core.tests.tests.TestApplication.test_aventri`

## Release Process

### Linting

A tool called Pre-commit is installed, which runs linting when committing.

### Commit Signing

Commits must be signed to be submitted to github. Use GPG Signer [FIND LINK]

### Deployment

Deploy master branch or feature branches to development, staging or production here:

https://jenkins.ci.uktrade.io/job/activity-stream/

## Dashboards and Metrics

Due to the quantity of data, being passed, the logs (and any individual request) makes it difficult to interpret the wider picture. Thus dashboards are used as the first source of debugging and to understand if a deployment has been successful.

Staging Dashboard: https://grafana.ci.uktrade.io/d/gKcrzUKmz/activity-stream?orgId=1&var-instance=activity-stream-staging.london.cloudapps.digital:443

Two key graphs to use are the "Feed failures in previous minute" graph, and the "Age of youngest activity" graph. Feed Failures graph shows whether feeds are connected and active correctly, and should show zero for all feeds. The "Age of youngest activity" shows the age of the most recent activity, and generally should trend flat. If the age is going up, no activities are being consumed and this indicates that the "outgoing" script is down.

## Logging

Logging is the best way.

Logs are usually output in the format

    [application name,optional comma separated contexts] Action... (completion status)

For example two message involved in gathering metrics would be shown in Kibana as

    August 12th 2018, 10:59:45.829  [outgoing,metrics] Saving to Redis... (done)
    August 12th 2018, 10:59:45.827  [outgoing,metrics] Saving to Redis...

There are potentially many separate chains of concurrent behaviour at any given time: the context is used to help quickly distinguish them in the logs. For the outpoing application when it fetches data, the context is a human-readable unique identifier for the source. For incoming requests, the context is a unique identifier generated at the beginning of the request.

    [elasticsearch-proxy,IbPY5ozS] Elasticsearch request by (ACCOUNT_ID) to (GET) (dev-v6qrmm4neh44yfdng3dlj24umu.eu-west-1.es.amazonaws.com:443/_plugin/kibana/bundles/one)...
    [elasticsearch-proxy,IbPY5ozS] Receiving request (10.0.0.456) (GET /_plugin/kibana/bundles/commons.style.css?v=16602 HTTP/1.1) (Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36) (1.2.3.4, 127.0.0.1)

To view the logs, use a command such as `cf logs activity-stream-staging | grep selling_online_overseas_markets`


## How Project Works

Activity Stream has two main applications. A script called "Outgoing" collects data from each project and saves it in elasticsearch. An API called "Incoming" provides access to this data.

### The "Outgoing" Script

Each data type is saved in a seperate index inside Activity Stream, e.g. one index for Opportunties, one for Articles, one for Markets, and so on.

In fact, each item is saved twice, first in one index for Activities and one for Objects. The difference is that the "Activities" index stores each action performed on the object ("Created, Updated") as a different item and so a given object can appear multiple times. The "Objects" index for a particular data type only stores the most up to date version of a given object, so each item in the index is a unique object.

Each project to be connected to Activity Stream needs an API that the Activity Stream "Outgoing" script can crawl. For consistency the data should be presented in W3C Activity 2.0 format. Specifically, it must offer a `Collection` with the activites in the `orderedItems` key. The API in each project should provide the data as a list of all data, ordered by some attribute (e.g. date updated), one page at a time. Each "page" of data also provides a link to the next page in the chain in the key `next`, until the last page which does not have a `next` key.

As it consumes an API, the Outgoing Script will create a new pair of temporary indices (one for activities and one for objects) for each source. When the script has consumed the last page of data, the current pair of indexes are deleted and replaced by the just-created temporary indices, and the process immediately begins again from the first page. The script is not polling the source on a scheduled basis, but rather continuously and repeatedly downloading the data.

Repeatedly downloading the data keeps the data in Activity Stream up to date. If there are any errors in the script, it restarts. Because Activity Stream deletes any data shortly after the source does, Activity Stream is also GDPR compliant.

The Outbound script does not know about the format of the data that is consumed or provided. When the format of the data changes, "Outgoing" automatically deletes the old data and replaces it with the new format data after one cycle.

### The "Incoming" API

The Inbound API contains an endpoint for consuming all activities that meet a certain Elasticsearch Query, which is provided at /v1/activities. Results are returned one page at a time. Also, an API for

### Authentication

Both "Incoming" and "Outgoing" use Hawk Authentication.

### Technical Notes

- Outgoing is not scalable, besides deployments there should only be one instance running.
- Outgoing is self-correcting after any downtime of either Activity Stream or the source.
- Each activity must have an `id` field that is unique. [In the current implementation, it doesn't have to be unique accross all sources, but this should be treated as an implementation detail. It is recommend to ensure this is unique accross all sources.]


### Performance requirements of source APIs

The Outgoing script only makes a single connection to the source service at any given time, and will not follow the next page of pagination until the previous one is retrieved. It continuously puts one extra user's worth of load on the source service. The source APIs should be able to handle concurrent users to support this.

The extra user performs a database query multiple times every second. For this reason it's important to optimise the performance of the source APIs. For example, paginating using a simple offset/limit in a SQL query is extremely inefficient on later pages. Pagination based on a combination of create/update timestamp and primary key, together with appropriate indexes in the database, works well. See one of the existing projects for an example of this.

It is possible to rate limit the Activity Stream if necessary. The source API should respond with HTTP 429, containing a `Retry-After` header containing how after many seconds the Activity Stream should retry the same URL.



### The Update Cycle

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

In tests, this results in updates being available in Elasticsearch in between 1 and 4 seconds.


### Techniques for updates / deletion

Each activity is ingested into Elasticsearch into a document with the `id` of the activity. As is typical with Elasticsearch, ingest of a document completely overwrites any existing one with that `id`.  This gives a method for updates or deletions:

- Activities can be updated, including redaction of any private data, by ensuring an activity is output with the same `id` as the original. This does not necessarily have to wait for the next full ingest, as long as the activity appears on the page being polled for updates.

Alternatively, because each full ingest is into a new index:

- Activities can be completely deleted by making sure they do not appear on the next full ingest. The limitation of this is that this won't take effect in Elasticsearch until the next full ingest is complete.

### Duplicates

The paginated feed can output the same activity multiple times, and as long as each has the same `id`, it won't be repeated in Elasticsearch.




## Verification Feed


## Local development environment

Python 3.6 is required. You can set this up using conda.

```bash
conda create -n python36 python=3.6 anaconda
conda activate python36
```

### Running tests

You must have the dependencies specified in [core/requirements_test.txt](core/requirements_test.txt), which can be installed by

```bash
pip install -r core/requirements_test.txt
```

Elasticsearch and Redis must be started before the tests, which you can do by

    ./tests_es_start.sh && ./tests_redis_start.sh

and then to run the tests themselves

    ./tests.sh

Higher-level feature tests, that assume very little over the internals of the application, are strongly preferred over lower-level tests that test individual functions, often called unit tests. This involves real Elasticsearch and Redis, and firing up various HTTP servers inside the test environment: since this is how the application interacts with the world in production. Mocking/patching is done for speed or determinism reasons: e.g. patching `asyncio.sleep` or `os.urandom`.

### Running locally


This closely resembles how the CI pipeline builds and deploys the applications.

## Development and git history

Aim to make

- `git log` and `git blame` helpful;
- interruptions, changing requirements, and multiple people working on the same code, painless.

To achieve this, wherever possible

- PRs should also be small. A max of ~1 day of work is a reasonable guideline;
- each commit in the PR should be small;
- each commit message should explain _why_ the change was done;
- tests should be in the same commit as the relevant production code change
- all tests should pass for each commit;
- documentation changes should be in the same commit as the relavant production code change;
- refactoring to support a production code behaviour change should be in separate commits, and ideally come _before_ commits the behaviour change in the final `git log`;
- you should take whatever steps you deem necessary in terms of tests or QA so that the master branch can be released with confidence after a merge of a PR;
- keep a linear / semi-linear git history: avoid `git merge master` when on a feature branch.

To achieve the above, you are likely to need

- `git rebase -i`;
- `git rebase master`;
- `git push origin my-feature-branch -f`.

Keeping commits small, releasable, and with working tests is especially helpful to make sure things don't go wrong with the above.

## Code guidelines

- Clarity of the data flow and transformations is paramount: classes are only used when there isn't a reasonable alternative.
- Each variable should be given a meaningful and useful value in all cases: often the ternary operator is used for this.
- Mutation is kept to a minimum. Ideally once set, a variable's value will not change.
- If a variable's value does change, it should have the same type.
- Ideally every line of code in a function is used by every caller of the function. Exceptions include functions that use guard conditions to return early as part of validation.
- Optional arguments are avoided in favour of consistency of the use of each function: all arguments are made explict by all callers.
- Comments should be kept to a minimum: refactoring code to make it self-documenting is preferred.
- External dependencies are added only if there is a very good argument over alternatives. Ideally as time goes on, the amount of external dependencies will go down, not up.
- Code should be designed for the _current_ behaviour in terms of inputs and outputs, and avoid anything that isn't required for this. Be _very_ self-skeptical if you're adding complexity for perceived future requirements. Minimise complexity by only adding it when it's needed, rather than just in case.

These guidelines also apply to test code, since the behaviour of production code is enforced, in part, by the tests.

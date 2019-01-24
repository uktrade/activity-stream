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
  - the URL for the next page is given explicitly in the page;
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

## Source HTTP endpoints

### Requirements

- Starting from a seed URL, search source HTTP endpoint must output activities in Activity Streams 2.0 JSON-LD format. Specifically, it must offer a `Collection` with the activites in the `orderedItems` key.
- It must allow pagination, giving the full URL to the next page of results giving a full URL in the `next` key. The final page should not have a `next` key. The Activity Stream itself doesn't depend on what method is used for pagination, but care must be taken to not put too much pressure on the underlying database. For example, paginating using a simple offset/limit in a SQL query is likely to be extremely inefficient on later pages. However, pagination based on a combination of create/update timestamp and primary key, together with appropriate indexes in the database, is more likely to be suitable.
- It should be expected that the final page, without a `next` key, will be polled for latest updates once a second. If a `next` key is later set in the results of this page, it will be paginated through until it reaches a page without the `next` key set.
- Each activity must have an `id` field that is unique. [In the current implementation, it doesn't have to be unique accross all sources, but this should be treated as an implementation detail. It is recommend to ensure this is unique accross all sources.]

### Techniques for updates / deletion

Each activity is ingested into Elasticsearch into a document with the `id` of the activity. As is typical with Elasticsearch, ingest of a document completely overwrites any existing one with that `id`.  This gives a method for updates or deletions:

- Activities can be updated, including redaction of any private data, by ensuring an activity is output with the same `id` as the original. This does not necessarily have to wait for the next full ingest, as long as the activity appears on the page being polled for updates.

Alternatively, because each full ingest is into a new index:

- Activities can be completely deleted by making sure they do not appear on the next full ingest. The limitation of this is that this won't take effect in Elasticsearch until the next full ingest is complete.

### Duplicates

The paginated feed can output the same activity multiple times, and as long as each has the same `id`, it won't be repeated in Elasticsearch.

### Rate Limiting

The Activity Stream should only make a single connection to the source service at any given time, and will not follow the next page of pagination until the previous one is retrieved. The source service should be designed to be able to handle these in succession: a standard web application that can handle concurrent users would typically be sufficient.

However, it is possible to rate limit the Activity Stream if it's necessary.

- Responding with HTTP 429, containing a `Retry-After` header containing how after many seconds the Activity Stream should retry the same URL.

- Responding with a HTTP code >= 400, that isn't a 429. The Activity Stream will treat this as a failed ingestion, and start again from the seed after 1 second. The time increases exponentially with each consecutive failure, until maxing out at 64 seconds between retries. The process also occurs on general HTTP connection issues.

## Outgoing and incoming applications

The core of the Activity Stream is made of two applications.

### Outgoing

This is the application that performs the above algorithm, continually making <em>outgoing</em> HTTP connections to pull data. It is not scalable, and other than during deployments, there should only be one instance running at any time. However, it has low CPU requirements, and as explained above, self-correcting after any downtime.

### Incoming

This is the application that features a HTTP server, accepting <em>incoming</em> HTTP requests, and passes requests for data to Elasticsearch. It converts the raw Elasticsearch format returned into a Activity Streams 2.0 compatible format. This is scalable, and multiple instances of this application can be running at any given time.

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

Since the tests are fairly high level, most development should be able to be done without starting the application separately. However, if you do wish to run the application locally, you must have the dependencies specified in [core/requirements.txt](core/requirements.txt), which can be installed by

```bash
pip install -r core/requirements.txt
```

and have a number of environment variables set: the up-to-date list of these are in the `mock_env` function defined in [tests_utils.py](core/tests_utils.py). Then to run the application that polls feeds

    (cp -r -f shared core && cd core && python -m app.app_outgoing)

or to run the application that proxies incoming requests to Elasticsearch

    (cp -r -f shared core && cd core && python -m app.app_incoming)

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
- Ideally every line of code in a function is used by every caller of the function. Exceptions include functions that "guard conditions" that return early as part of validation.
- Comments should be kept to a minimum: refactoring code to make it self-documenting is preferred.

These guidelines also apply to test code, since the behaviour of production code is enforced, in part, by the tests.

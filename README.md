# activity-stream [![CircleCI](https://circleci.com/gh/uktrade/activity-stream.svg?style=svg)](https://circleci.com/gh/uktrade/activity-stream)

Activity Stream is a collector of various interactions between people and DIT.

## Running tests

    ./tests.sh

## Managing Requirements

When adding a new library, first add it to requirements.in, then::

    pip install pip-tools
    pip-compile --output-file requirements.txt requirements.in
    pip install -r requirements.txt

## Database

We use Postgres 9.5; for convenience you can start an instance via Docker:

    docker-compose up postgres
    
This will automatically create a database called `activity-stream`; the username and
password are the default: postgres / postgres.
    
There is only one related environment variable - `DATABASE_URL` - which is mandatory; if using
the above instance you can set it like so:
    
    export DATABASE_URL='postgres://postgres@localhost/activity-stream'
    
Please note that, if deploying to Gov PaaS, this will be set automatically.  


## Endpoints

### Recording an action

```
curl -i -X POST http://localhost:8000/api/actions/feedback/ \
-H 'Content-Type: text/json; charset=utf-8' \
-d @- <<'EOF'
{
   "source": "directory",
   "occurrence_date": "2012-04-23T18:25:43.511Z",
   "actor_name": "Bob Roberts",
   "actor_email_address": "bob.roberts@somewhere.io",
   "actor_business_sso_uri": null,
   "details": [
      {
         "key": "originating_page",
         "value": "Direct request"
      },
      {
         "key": "message",
         "value": "Lorem ipsum dolor sit amet, consectetur adipiscing elit"
      }
   ]
}
EOF
```

```
HTTP/1.1 201 Created
Date: Fri, 23 Mar 2018 11:33:53 GMT
Server: WSGIServer/0.2 CPython/3.6.4
Content-Type: text/html; charset=utf-8
Location: http://localhost:8000/api/actions/26
Vary: Accept
Allow: POST, OPTIONS
```

Please note the returned location of the new Action: `http://localhost:8000/actions/26`.
 
### Reading an action
 
 ``` 
 curl http://localhost:8000/api/actions/26 | jq
 ```
 
 ``` 
 {
  "action_type": {
    "id": 2,
    "name": "feedback"
  },
  "action_details": [
    {
      "key": "originating_page",
      "value": "Direct request"
    },
    {
      "key": "message",
      "value": "Lorem ipsum dolor sit amet, consectetur adipiscing elit"
    }
  ],
  "actor_name": "Bob Roberts",
  "actor_email_address": "bob.roberts@somewhere.io",
  "actor_business_sso_uri": null,
  "id": 25,
  "occurrence_date": "2012-04-23T18:25:43.511000Z",
  "recording_date": "2018-03-23T11:13:25.908794Z",
  "source": "directory"
}
 ```
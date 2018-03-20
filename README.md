# activity-stream

Activity Stream is a collector of various interactions between contacts at companies.

## Managing Requirements

When adding a new library, first add it to requirements.in, then::

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
#!/bin/bash
set -x

env | grep '^PG'

# If you want to run this script for your own postgresql (run with
# docker-compose) it will look like this:
# PGHOST=127.0.0.1 PGUSER=root PGPASSWORD=password PGDATABASE=postgres \
PGUSER="${PGUSER:-postgres}"
export PGUSER
PGPORT="${PGPORT:-5432}"
export PGPORT
PGHOST="${PGHOST:-localhost}"

until psql -h "$PGHOST" -c '\q'; do
  >&2 echo "Postgres is unavailable - sleeping"
  sleep 1
done

createdb dbt
psql -c "CREATE ROLE root WITH PASSWORD 'password';"
psql -c "ALTER ROLE root WITH LOGIN;"
psql -c "GRANT CREATE, CONNECT ON DATABASE dbt TO root WITH GRANT OPTION;"

psql -c "CREATE ROLE noaccess WITH PASSWORD 'password' NOSUPERUSER;"
psql -c "ALTER ROLE noaccess WITH LOGIN;"
psql -c "GRANT CONNECT ON DATABASE dbt TO noaccess;"

psql -c 'CREATE DATABASE "dbtMixedCase";'
psql -c 'GRANT CREATE, CONNECT ON DATABASE "dbtMixedCase" TO root WITH GRANT OPTION;'

set +x

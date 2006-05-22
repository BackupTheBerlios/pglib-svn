#! /usr/bin/env sh

# execute commands in the setup.sql
psql -U postgres -d template1 -f setup.sql

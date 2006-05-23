#! /usr/bin/env sh

# execute commands in the setup.sql
psql -U postgres -d template1 -f setup.sql

# make sure that the PL/pgSQL language is installed
createlang -U postgres plpgsql pglib

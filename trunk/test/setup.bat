psql -U postgres -d template1 -f setup.sql
createlang -U postgres plpgsql pglib

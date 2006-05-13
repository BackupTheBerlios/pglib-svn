/* SQL setup for enable testing pglib with a PostgreSQL database.

Run this commands in the template1 database, as superuser.
*/


-- test database
CREATE DATABASE pglib;

-- test users, each with a different authentication type
CREATE USER pglib; -- no authentication
CREATE USER pglib_clear WITH PASSWORD 'test'; -- clear text password
CREATE USER pglib_md5 WITH PASSWORD 'test'; -- md5 password
CREATE USER pglib_ssl WITH PASSWORD 'test'; -- SLL required, clear text password
CREATE USER pglib_nossl WITH PASSWORD 'test'; -- SLL forbidden, md5 password

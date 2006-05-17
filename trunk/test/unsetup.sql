/* SQL commands for remove support for testing pglib with a PostgreSQL database.

Run this commands in the template1 database, as superuser.
*/


DROP USER pglib;
DROP USER pglib_clear;
DROP USER pglib_md5;
DROP USER pglib_ssl;
DROP USER pglib_nossl;

DROP DATABASE pglib;

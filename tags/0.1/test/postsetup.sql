/* Script with test functions

Execute with psql -U pglib -d pglib
*/

-- simple function
CREATE OR REPLACE FUNCTION echo(text) RETURNS text AS '
BEGIN
        RETURN $1;
END;
' LANGUAGE plpgsql;


-- function used to test cancel
CREATE OR REPLACE FUNCTION loop() RETURNS void AS '
BEGIN
        LOOP
        END LOOP;
END;
' LANGUAGE plpgsql;


-- some table
DROP TABLE TestRW;
DROP TABLE TestR;
DROP TABLE TestCopyR;
DROP TABLE TestCopyRW;


CREATE TABLE TestRW (
       x INTEGER,
       s TEXT
);

CREATE TABLE TestR (
       x INTEGER,
       s TEXT
);

CREATE TABLE TestCopyR (
       x INTEGER,
       s TEXT
);

CREATE TABLE TestCopyRW (
       x INTEGER,
       s TEXT
);


INSERT INTO TestR VALUES (1, 'A');
INSERT INTO TestR Values (2, 'B');

INSERT INTO TestRW VALUES (1, 'A');
INSERT INTO TestRW Values (2, 'B');


COPY TestCopyR (x, s) FROM STDIN WITH DELIMITER '|';
1|pglib
2|manlio
3|perillo
\.


-- privileges setup
GRANT ALL PRIVILEGES ON TestR TO PUBLIC;
GRANT ALL PRIVILEGES ON TestRW TO PUBLIC;
GRANT ALL PRIVILEGES ON TestCopyR TO PUBLIC;
GRANT ALL PRIVILEGES ON TestCopyRW TO PUBLIC;

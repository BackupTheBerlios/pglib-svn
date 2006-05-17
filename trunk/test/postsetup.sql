/* Script with test functions

Execute with psql -U pglib -d pglib
*/

-- simple function
CREATE FUNCTION echo(data text) RETURNS text AS $$
BEGIN
        RETURN data;
END;
$$ LANGUAGE plpgsql;


-- function used to test cancel
CREATE FUNCTION loop() RETURNS void AS $$
BEGIN
        LOOP
        END LOOP;
END;
$$ LANGUAGE plpgsql;


-- some table
CREATE TABLE TestRW (
       x INTEGER,
       s TEXT
);

CREATE TABLE TestR (
       x INTEGER,
       s TEXT
);


INSERT INTO TestR VALUES (1, 'A');
INSERT INTO TestR Values (2, 'B');


-- privileges setup
GRANT ALL PRIVILEGES ON TestR TO PUBLIC;
GRANT ALL PRIVILEGES ON TestRW TO PUBLIC;

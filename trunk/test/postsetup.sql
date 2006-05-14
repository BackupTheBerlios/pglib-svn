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


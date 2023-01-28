CREATE EXTENSION IF NOT EXISTS postgres_fdw;

CREATE FUNCTION clean_up() RETURNS void
AS 
$$
  DROP SCHEMA IF EXISTS public_foreign CASCADE;
  DROP USER MAPPING IF EXISTS FOR postgres SERVER fdw_foreign;
  DROP SERVER IF EXISTS fdw_foreign;
$$
LANGUAGE SQL;

SELECT clean_up();

CREATE SERVER fdw_foreign FOREIGN DATA WRAPPER postgres_fdw OPTIONS (host 'localhost', dbname 'dwbif_source', port '5432');

CREATE USER MAPPING FOR postgres SERVER fdw_foreign OPTIONS (user 'postgres', password 'postgres');

CREATE SCHEMA public_foreign;

IMPORT FOREIGN SCHEMA public FROM SERVER fdw_foreign INTO public_foreign;


-- QUERY SECTION


-- END QUERY SECTION


SELECT clean_up();

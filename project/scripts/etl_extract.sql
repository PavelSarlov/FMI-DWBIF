CREATE EXTENSION IF NOT EXISTS postgres_fdw;

CREATE OR REPLACE FUNCTION clean_up() RETURNS void
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


INSERT INTO public.account
SELECT NOW(), * FROM public_foreign.account;

INSERT INTO public.client
SELECT NOW(), * FROM public_foreign.client;

INSERT INTO public.credit_card
SELECT NOW(), * FROM public_foreign.credit_card;

INSERT INTO public.demographic_data
SELECT NOW(), * FROM public_foreign.demographic_data;

INSERT INTO public.disposition
SELECT NOW(), * FROM public_foreign.disposition;

INSERT INTO public.loan
SELECT NOW(), * FROM public_foreign.loan;

INSERT INTO public.permanent_order
SELECT NOW(), * FROM public_foreign.permanent_order;

INSERT INTO public.transaction
SELECT NOW(), * FROM public_foreign.transaction;


-- END QUERY SECTION


SELECT clean_up();

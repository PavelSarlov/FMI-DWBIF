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

CREATE SERVER fdw_foreign FOREIGN DATA WRAPPER postgres_fdw OPTIONS (host 'localhost', dbname 'dwbif_warehouse', port '5432');

CREATE USER MAPPING FOR postgres SERVER fdw_foreign OPTIONS (user 'postgres', password 'postgres');

CREATE SCHEMA public_foreign;

IMPORT FOREIGN SCHEMA public FROM SERVER fdw_foreign INTO public_foreign;


-- QUERY SECTION


-- DIMENSIONS

WITH "date_list" AS (
  SELECT DISTINCT("date") FROM 
  (
    SELECT "date" from public_foreign.s_account
    UNION
    SELECT "date" from public_foreign.s_loan
    UNION
    SELECT "date" from public_foreign.s_transaction
  ) a
)
INSERT INTO public.d_date(year, month, day)
SELECT date_part('year', "date"), date_part('month', "date"), date_part('day', "date")
FROM "date_list";

DO $$
BEGIN
  FOR hour IN 0..23 LOOP
    FOR minute IN 0..59 LOOP
      INSERT INTO d_time(hour, minute) VALUES(hour, minute);
    END LOOP;
  END LOOP;
END $$;

INSERT INTO public.d_client(id, client_id, birth_number)
SELECT h.id, h.client_id, s.birth_number 
FROM public_foreign.h_client h
  JOIN public_foreign.s_client s ON s.client_id = h.id;

INSERT INTO public.d_account(id, account_id, frequency)
SELECT h.id, h.account_id, raf.value 
FROM public_foreign.h_account h
  JOIN public_foreign.s_account s ON s.account_id = h.id
  LEFT JOIN public_foreign.r_account_frequency raf ON raf.id = s.frequency

INSERT INTO public.d_disposition(id, disp_id, "type")
SELECT h.id, h.disp_id, rdt.value 
FROM public_foreign.h_disposition h
  JOIN public_foreign.s_disposition s ON s.disposition_id = h.id
  LEFT JOIN public_foreign.r_disposition_type rdt ON rdt.id = s."type";

INSERT INTO public.d_credit_card(id, card_id, "type")
SELECT h.id, h.card_id, r.value
FROM public_foreign.h_credit_card h
  JOIN public_foreign.s_credit_card s ON s.card_id = h.id
  LEFT JOIN public_foreign.r_credit_card_type r ON r.id = s."type";

INSERT INTO public.d_demographic(
  id,
  district_id,
  district_name,
  region,
  inhabitants,
  municip_inhabitants_0_499,
  municip_inhabitants_500_1999,
  municip_inhabitants_2000_9999,
  municip_inhabitants_10000_inf,
  cities,
  urban_inhabitants_ratio,
  avg_salary,
  unemployment_rate_95,
  unemployment_rate_96,
  enterpreneurs_per_1000_inhabitants,
  commited_crimes_95,
  commited_crimes_96
)
SELECT 
  h.id,
  h.district_id,
  s.district_name,
  s.region,
  s.inhabitants,
  s.municip_inhabitants_0_499,
  s.municip_inhabitants_500_1999,
  s.municip_inhabitants_2000_9999,
  s.municip_inhabitants_10000_inf,
  s.cities,
  s.urban_inhabitants_ratio,
  s.avg_salary,
  s.unemployment_rate_95,
  s.unemployment_rate_96,
  s.enterpreneurs_per_1000_inhabitants,
  s.commited_crimes_95,
  s.commited_crimes_96
FROM public_foreign.h_demographic h
  JOIN public_foreign.s_demographic s ON s.demographic_id = h.id;

INSERT INTO public.d_loan(id, loan_id, duration, status)
SELECT h.id, h.loan_id, s.duration, r.value 
FROM public_foreign.h_loan h
  JOIN public_foreign.s_loan s ON s.loan_id = h.id
  LEFT JOIN public_foreign.r_loan_status r ON r.id = s.status;

INSERT INTO public.d_transaction(id, trans_id, k_symbol, bank, account, "type", operation)
SELECT h.id, h.trans_id, rks.value, rp.bank, rp.account, rtt.value, rto.value
FROM public_foreign.h_transaction h
  JOIN public_foreign.s_transaction s ON s.transaction_id = h.id
  LEFT JOIN public_foreign.r_transaction_type rtt ON rtt.id = s."type"
  LEFT JOIN public_foreign.r_transaction_operation rto ON rto.id = s.operation
  LEFT JOIN public_foreign.r_partner rp ON rp.id = s.partner
  LEFT JOIN public_foreign.r_k_symbol rks ON rks.id = s.k_symbol;

INSERT INTO public.d_permanent_order(id, order_id, k_symbol, bank, account)
SELECT h.id, h.order_id, rks.value, rp.bank, rp.account 
FROM public_foreign.h_permanent_order h
  JOIN public_foreign.s_permanent_order s ON s.permanent_order_id = h.id
  LEFT JOIN public_foreign.r_partner rp ON rp.id = s.partner
  LEFT JOIN public_foreign.r_k_symbol rks ON rks.id = s.k_symbol;

-- END DIMENSIONS

-- FACTS

INSERT INTO public.f_credit_card(card_id, account_id, client_id, issued_date, issued_time)
SELECT 
  hcc.id, 
  account_id, 
  client_id, 
  dd.id,
  dt.id
FROM public_foreign.l_client_account_disposition lcad
  JOIN public_foreign.l_disposition_credit_card ldcc ON ldcc.id = lcad.disposition_id
  JOIN public_foreign.h_credit_card hcc ON hcc.id = ldcc.card_id
  JOIN public_foreign.s_credit_card scc ON scc.card_id = hcc.id
  JOIN public.d_date dd ON dd.year = date_part('year', issued) AND dd.month = date_part('month', issued) AND dd.day = date_part('day', issued)
  JOIN public.d_time dt ON dt.hour = date_part('hour', issued) AND dt.minute = date_part('minute', issued);

INSERT INTO public.f_client(demographic_id, client_id)
SELECT 
  demographic_id,
  client_id
FROM public_foreign.l_client_demographic lcd;

INSERT INTO public.f_account(account_id, client_id, demographic_id, disposition_id, "date")
SELECT 
  lad.account_id,
  client_id,
  demographic_id,
  disposition_id,
  dd.id
FROM public_foreign.l_account_demographic lad
  JOIN public_foreign.l_client_account_disposition lcad ON lcad.account_id = lad.account_id
  JOIN public_foreign.h_account ha ON ha.id = lad.account_id
  JOIN public_foreign.s_account sa ON sa.account_id = ha.id
  JOIN public.d_date dd ON dd.year = date_part('year', sa."date") AND dd.month = date_part('month', sa."date") AND dd.day = date_part('day', sa."date");

INSERT INTO public.f_loan(account_id, loan_id, "date", amount, payments)
SELECT 
  lal.account_id,
  lal.loan_id,
  dd.id,
  amount,
  payments
FROM public_foreign.l_account_loan lal
  JOIN public_foreign.h_loan hl ON hl.id = lal.loan_id
  JOIN public_foreign.s_loan sl ON sl.loan_id = hl.id
  JOIN public.d_date dd ON dd.year = date_part('year', sl."date") AND dd.month = date_part('month', sl."date") AND dd.day = date_part('day', sl."date");

INSERT INTO public.f_transaction(transaction_id, account_id, "date", amount, balance)
SELECT 
  lat.transaction_id,
  account_id,
  dd.id,
  amount,
  balance
FROM public_foreign.l_account_transaction lat
  JOIN public_foreign.h_transaction ht ON ht.id = lat.transaction_id
  JOIN public_foreign.s_transaction st ON st.transaction_id = ht.id
  JOIN public.d_date dd ON dd.year = date_part('year', st."date") AND dd.month = date_part('month', st."date") AND dd.day = date_part('day', st."date");

INSERT INTO public.f_permanent_order(account_id, order_id, amount)
SELECT 
  account_id,
  lapo.permanent_order_id,
  amount
FROM public_foreign.l_account_permanent_order lapo
  JOIN public_foreign.h_permanent_order hpo ON hpo.id = lapo.permanent_order_id
  JOIN public_foreign.s_permanent_order spo ON spo.permanent_order_id = hpo.id;

-- END FACTS


-- END QUERY SECTION


SELECT clean_up();

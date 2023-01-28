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

CREATE SERVER fdw_foreign FOREIGN DATA WRAPPER postgres_fdw OPTIONS (host 'localhost', dbname 'dwbif_staging', port '5432');

CREATE USER MAPPING FOR postgres SERVER fdw_foreign OPTIONS (user 'postgres', password 'postgres');

CREATE SCHEMA public_foreign;

IMPORT FOREIGN SCHEMA public FROM SERVER fdw_foreign INTO public_foreign;


-- QUERY SECTION

-- REFERENCE TABLES

INSERT INTO public.r_account_frequency(value)
SELECT DISTINCT(frequency) FROM public_foreign.account
WHERE frequency IS NOT NULL AND frequency <> '';

INSERT INTO public.r_credit_card_type(value)
SELECT DISTINCT("type") FROM public_foreign.credit_card
WHERE "type" IS NOT NULL AND "type" <> '';

INSERT INTO public.r_disposition_type(value)
SELECT DISTINCT("type") FROM public_foreign.disposition
WHERE "type" IS NOT NULL AND "type" <> '';

INSERT INTO public.r_k_symbol(value)
SELECT * FROM 
(SELECT DISTINCT(TRIM(k_symbol)) AS k_symbol FROM public_foreign.transaction
UNION
SELECT DISTINCT(TRIM(k_symbol)) AS k_symbol FROM public_foreign.permanent_order) k
WHERE k_symbol IS NOT NULL AND k_symbol <> '';

INSERT INTO public.r_loan_status(value)
SELECT DISTINCT(status) FROM public_foreign.loan
WHERE status IS NOT NULL AND status <> '';

INSERT INTO public.r_partner(bank, account)
SELECT * FROM 
(SELECT DISTINCT ON (bank_to, account_to) bank_to AS bank, account_to AS account FROM public_foreign.permanent_order
UNION
SELECT DISTINCT ON (bank, account) bank, account FROM public_foreign.transaction) k
WHERE bank IS NOT NULL AND bank <> '';

INSERT INTO public.r_transaction_operation(value)
SELECT DISTINCT(operation) FROM public_foreign.transaction
WHERE operation IS NOT NULL AND operation <> '';

INSERT INTO public.r_transaction_type(value)
SELECT DISTINCT("type") FROM public_foreign.transaction
WHERE "type" IS NOT NULL AND "type" <> '';

-- END REFERENCE TABLES

-- HUBS AND SATELLITES

WITH hub_helper AS (
  INSERT INTO public.h_demographic(loaded_at, source, district_id)
  SELECT NOW(), 'localhost:5432:dwbif_staging', a1 
  FROM (
    SELECT a1, staged_at, MAX(staged_at) OVER (PARTITION BY a1) AS most_recent
    FROM public_foreign.demographic_data
  ) src
  WHERE src.staged_at = src.most_recent
  RETURNING * 
)
INSERT INTO public.s_demographic
SELECT hh.id, hh.loaded_at, hh.source, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16 
FROM public_foreign.demographic_data pf
  JOIN hub_helper hh ON hh.district_id = pf.a1;

WITH hub_helper AS (
  INSERT INTO public.h_account(loaded_at, source, account_id)
  SELECT NOW(), 'localhost:5432:dwbif_staging', account_id 
  FROM (
    SELECT account_id, staged_at, MAX(staged_at) OVER (PARTITION BY account_id) AS most_recent
    FROM public_foreign.account
  ) src
  WHERE src.staged_at = src.most_recent
  RETURNING * 
)
INSERT INTO public.s_account
SELECT hh.id, hh.loaded_at, hh.source, DATE("date"::text), raf.id
FROM public_foreign.account pf
  JOIN hub_helper hh ON hh.account_id = pf.account_id
  LEFT JOIN r_account_frequency raf ON raf.value ILIKE pf.frequency;

WITH hub_helper AS (
  INSERT INTO public.h_loan(loaded_at, source, loan_id)
  SELECT NOW(), 'localhost:5432:dwbif_staging', loan_id
  FROM (
    SELECT loan_id, staged_at, MAX(staged_at) OVER (PARTITION BY loan_id) AS most_recent
    FROM public_foreign.loan
  ) src
  WHERE src.staged_at = src.most_recent
  RETURNING * 
)
INSERT INTO public.s_loan
SELECT hh.id, hh.loaded_at, hh.source, DATE("date"::text), amount, duration, payments, rls.id
FROM public_foreign.loan pf
  JOIN hub_helper hh ON hh.loan_id = pf.loan_id
  LEFT JOIN r_loan_status rls ON rls.value ILIKE pf.status;

WITH hub_helper AS (
  INSERT INTO public.h_permanent_order(loaded_at, source, order_id)
  SELECT NOW(), 'localhost:5432:dwbif_staging', order_id
  FROM (
    SELECT order_id, staged_at, MAX(staged_at) OVER (PARTITION BY order_id) AS most_recent
    FROM public_foreign.permanent_order
  ) src
  WHERE src.staged_at = src.most_recent
  RETURNING * 
)
INSERT INTO public.s_permanent_order
SELECT hh.id, hh.loaded_at, hh.source, rp.id, amount, rks.id
FROM public_foreign.permanent_order pf
  JOIN hub_helper hh ON hh.order_id = pf.order_id
  LEFT JOIN r_partner rp ON rp.bank ILIKE pf.bank_to AND rp.account = pf.account_to 
  JOIN r_k_symbol rks ON rks.value ILIKE pf.k_symbol;

WITH hub_helper AS (
  INSERT INTO public.h_transaction(loaded_at, source, trans_id)
  SELECT NOW(), 'localhost:5432:dwbif_staging', trans_id 
  FROM (
    SELECT trans_id, staged_at, MAX(staged_at) OVER (PARTITION BY trans_id) AS most_recent
    FROM public_foreign.transaction
  ) src
  WHERE src.staged_at = src.most_recent
  RETURNING * 
)
INSERT INTO public.s_transaction
SELECT hh.id, hh.loaded_at, hh.source, DATE("date"::text), rtt.id, rto.id, amount, balance, rks.id, rp.id
FROM public_foreign.transaction pf
  JOIN hub_helper hh ON hh.trans_id = pf.trans_id
  JOIN r_partner rp ON rp.bank ILIKE pf.bank AND rp.account = pf.account 
  LEFT JOIN r_transaction_type rtt ON rtt.value ILIKE pf."type"
  LEFT JOIN r_transaction_operation rto ON rto.value ILIKE pf.operation
  JOIN r_k_symbol rks ON rks.value ILIKE pf.k_symbol;

WITH hub_helper AS (
  INSERT INTO public.h_client(loaded_at, source, client_id)
  SELECT NOW(), 'localhost:5432:dwbif_staging', client_id
  FROM (
    SELECT client_id, staged_at, MAX(staged_at) OVER (PARTITION BY client_id) AS most_recent
    FROM public_foreign.client
  ) src
  WHERE src.staged_at = src.most_recent
  RETURNING * 
)
INSERT INTO public.s_client
SELECT hh.id, hh.loaded_at, hh.source, birth_number
FROM public_foreign.client pf
  JOIN hub_helper hh ON hh.client_id = pf.client_id;

WITH hub_helper AS (
  INSERT INTO public.h_disposition(loaded_at, source, disp_id)
  SELECT NOW(), 'localhost:5432:dwbif_staging', disp_id
  FROM (
    SELECT disp_id, staged_at, MAX(staged_at) OVER (PARTITION BY disp_id) AS most_recent
    FROM public_foreign.disposition
  ) src
  WHERE src.staged_at = src.most_recent
  RETURNING * 
)
INSERT INTO public.s_disposition
SELECT hh.id, hh.loaded_at, hh.source, rdt.id
FROM public_foreign.disposition pf
  JOIN hub_helper hh ON hh.disp_id = pf.disp_id
  LEFT JOIN r_disposition_type rdt ON rdt.value = pf."type";

WITH hub_helper AS (
  INSERT INTO public.h_credit_card(loaded_at, source, card_id)
  SELECT NOW(), 'localhost:5432:dwbif_staging', card_id
  FROM (
    SELECT card_id, staged_at, MAX(staged_at) OVER (PARTITION BY card_id) AS most_recent
    FROM public_foreign.credit_card
  ) src
  WHERE src.staged_at = src.most_recent
  RETURNING * 
)
INSERT INTO public.s_credit_card
SELECT hh.id, hh.loaded_at, hh.source, rcct.id, to_timestamp(issued, 'YYMMDD HH24:MI:SS')
FROM public_foreign.credit_card pf
  JOIN hub_helper hh ON hh.card_id = pf.card_id
  LEFT JOIN r_credit_card_type rcct ON rcct.value = pf."type";


-- END HUBS AND SATELLITES


-- LINKS


INSERT INTO public.l_account_demographic(loaded_at, source, account_id, demographic_id)
SELECT loaded_at, source, account_id, demographic_id
FROM (
  SELECT NOW() AS loaded_at, 'localhost:5432:dwbif_staging' AS source, ha.id AS account_id, hd.id AS demographic_id, ha.loaded_at AS ha_loaded_at, MAX(ha.loaded_at) OVER (PARTITION BY ha.id) AS ha_most_recent
  FROM public.h_account ha
    JOIN (
      SELECT *, MAX(staged_at) OVER (PARTITION BY account_id) AS most_recent
      FROM public_foreign.account
    ) pf_a ON pf_a.account_id = ha.account_id
    JOIN (
      SELECT *, MAX(loaded_at) OVER (PARTITION BY district_id) AS most_recent
      FROM public.h_demographic
    ) hd ON hd.district_id = pf_a.district_id
  WHERE pf_a.staged_at = pf_a.most_recent
   AND hd.loaded_at = hd.most_recent
) src
WHERE src.ha_loaded_at = src.ha_most_recent;

INSERT INTO public.l_client_demographic(loaded_at, source, client_id, demographic_id)
SELECT loaded_at, source, client_id, demographic_id
FROM (
  SELECT NOW() AS loaded_at, 'localhost:5432:dwbif_staging' AS source, ha.id AS client_id, hd.id AS demographic_id, ha.loaded_at AS ha_loaded_at, MAX(ha.loaded_at) OVER (PARTITION BY ha.id) AS ha_most_recent
  FROM public.h_client ha
    JOIN (
      SELECT *, MAX(staged_at) OVER (PARTITION BY client_id) AS most_recent
      FROM public_foreign.client
    ) pf_a ON pf_a.client_id = ha.client_id
    JOIN (
      SELECT *, MAX(loaded_at) OVER (PARTITION BY district_id) AS most_recent
      FROM public.h_demographic
    ) hd ON hd.district_id = pf_a.district_id
  WHERE pf_a.staged_at = pf_a.most_recent
   AND hd.loaded_at = hd.most_recent
) src
WHERE src.ha_loaded_at = src.ha_most_recent;

INSERT INTO public.l_account_loan(loaded_at, source, account_id, loan_id)
SELECT loaded_at, source, account_id, loan_id
FROM (
  SELECT NOW() AS loaded_at, 'localhost:5432:dwbif_staging' AS source, ha.id AS account_id, hd.id AS loan_id, ha.loaded_at AS ha_loaded_at, MAX(ha.loaded_at) OVER (PARTITION BY ha.id) AS ha_most_recent
  FROM public.h_account ha
    JOIN (
      SELECT *, MAX(staged_at) OVER (PARTITION BY loan_id) AS most_recent
      FROM public_foreign.loan
    ) pf_a ON pf_a.account_id = ha.account_id
    JOIN (
      SELECT *, MAX(loaded_at) OVER (PARTITION BY loan_id) AS most_recent
      FROM public.h_loan
    ) hd ON hd.loan_id = pf_a.loan_id
  WHERE pf_a.staged_at = pf_a.most_recent
   AND hd.loaded_at = hd.most_recent
) src
WHERE src.ha_loaded_at = src.ha_most_recent;

INSERT INTO public.l_account_permanent_order(loaded_at, source, account_id, permanent_order_id)
SELECT loaded_at, source, account_id, permanent_order_id
FROM (
  SELECT NOW() AS loaded_at, 'localhost:5432:dwbif_staging' AS source, ha.id AS account_id, hd.id AS permanent_order_id, ha.loaded_at AS ha_loaded_at, MAX(ha.loaded_at) OVER (PARTITION BY ha.id) AS ha_most_recent
  FROM public.h_account ha
    JOIN (
      SELECT *, MAX(staged_at) OVER (PARTITION BY order_id) AS most_recent
      FROM public_foreign.permanent_order
    ) pf_a ON pf_a.account_id = ha.account_id
    JOIN (
      SELECT *, MAX(loaded_at) OVER (PARTITION BY order_id) AS most_recent
      FROM public.h_permanent_order
    ) hd ON hd.order_id = pf_a.order_id
  WHERE pf_a.staged_at = pf_a.most_recent
   AND hd.loaded_at = hd.most_recent
) src
WHERE src.ha_loaded_at = src.ha_most_recent;

INSERT INTO public.l_account_transaction(loaded_at, source, account_id, transaction_id)
SELECT loaded_at, source, account_id, transaction_id
FROM (
  SELECT NOW() AS loaded_at, 'localhost:5432:dwbif_staging' AS source, ha.id AS account_id, hd.id AS transaction_id, ha.loaded_at AS ha_loaded_at, MAX(ha.loaded_at) OVER (PARTITION BY ha.id) AS ha_most_recent
  FROM public.h_account ha
    JOIN (
      SELECT *, MAX(staged_at) OVER (PARTITION BY trans_id) AS most_recent
      FROM public_foreign.transaction
    ) pf_a ON pf_a.account_id = ha.account_id
    JOIN (
      SELECT *, MAX(loaded_at) OVER (PARTITION BY trans_id) AS most_recent
      FROM public.h_transaction
    ) hd ON hd.trans_id = pf_a.trans_id
  WHERE pf_a.staged_at = pf_a.most_recent
   AND hd.loaded_at = hd.most_recent
) src
WHERE src.ha_loaded_at = src.ha_most_recent;

INSERT INTO public.l_client_account_disposition(loaded_at, source, account_id, disposition_id, client_id)
SELECT loaded_at, source, account_id, disposition_id, client_id
FROM (
  SELECT NOW() AS loaded_at, 'localhost:5432:dwbif_staging' AS source, ha.id AS account_id, hd.id AS disposition_id, hc.id as client_id, hd.loaded_at AS hd_loaded_at, MAX(hd.loaded_at) OVER (PARTITION BY hd.id) AS hd_most_recent
  FROM public.h_disposition hd
    JOIN (
      SELECT *, MAX(staged_at) OVER (PARTITION BY disp_id) AS most_recent
      FROM public_foreign.disposition
    ) pf_a ON pf_a.disp_id = hd.disp_id
    JOIN (
      SELECT *, MAX(loaded_at) OVER (PARTITION BY account_id) AS most_recent
      FROM public.h_account
    ) ha ON ha.account_id = pf_a.account_id
    JOIN (
      SELECT *, MAX(loaded_at) OVER (PARTITION BY client_id) AS most_recent
      FROM public.h_client
    ) hc ON hc.client_id = pf_a.client_id
  WHERE pf_a.staged_at = pf_a.most_recent
   AND ha.loaded_at = ha.most_recent
   AND hc.loaded_at = hc.most_recent
) src
WHERE src.hd_loaded_at = src.hd_most_recent;

INSERT INTO public.l_disposition_credit_card(loaded_at, source, disposition_id, card_id)
SELECT loaded_at, source, disposition_id, card_id
FROM (
  SELECT NOW() AS loaded_at, 'localhost:5432:dwbif_staging' AS source, hd.id AS disposition_id, hc.id as card_id, hc.loaded_at AS hc_loaded_at, MAX(hc.loaded_at) OVER (PARTITION BY hc.id) AS hc_most_recent
  FROM public.h_credit_card hc
    JOIN (
      SELECT *, MAX(staged_at) OVER (PARTITION BY card_id) AS most_recent
      FROM public_foreign.credit_card
    ) pf_a ON pf_a.card_id = hc.card_id
    JOIN (
      SELECT *, MAX(loaded_at) OVER (PARTITION BY disp_id) AS most_recent
      FROM public.h_disposition
    ) hd ON hd.disp_id = pf_a.disp_id
  WHERE pf_a.staged_at = pf_a.most_recent
   AND hd.loaded_at = hd.most_recent
) src
WHERE src.hc_loaded_at = src.hc_most_recent;


-- END LINKS


-- END QUERY SECTION


SELECT clean_up();

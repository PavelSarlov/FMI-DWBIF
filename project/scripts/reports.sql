-- Top 10 highest transaction payments for insurrance

SELECT DISTINCT * 
FROM (
  SELECT account_id, MAX(amount) OVER (PARTITION BY account_id) AS max_amount
  FROM f_transaction ft
    JOIN d_transaction dt ON ft.transaction_id = dt.id 
  WHERE k_symbol ILIKE 'POJISTNE'
) sub
ORDER BY max_amount DESC
LIMIT 10;

-- Clients grouped by gender, loan type (problematic/nonproblematic) and account frequency

CREATE OR REPLACE FUNCTION is_date(s VARCHAR) RETURNS BOOLEAN AS $$
BEGIN
  PERFORM s::date;
  RETURN TRUE;
EXCEPTION WHEN OTHERS THEN
  RETURN FALSE;
END;
$$ LANGUAGE PLPGSQL;

WITH men AS (
  SELECT id AS client_id
  FROM d_client
  WHERE is_date(birth_number::text)
), women AS (
  SELECT id AS client_id
  FROM d_client
  WHERE NOT is_date(birth_number::text)
), men_unproblematic_loans AS (
  SELECT 
    COUNT(*) FILTER (WHERE frequency ILIKE 'POPLATEK MESICNE') AS men_unproblematic_monthly_issuance,
    COUNT(*) FILTER (WHERE frequency ILIKE 'POPLATEK TYDNE') AS men_unproblematic_weekly_issuance,
    COUNT(*) FILTER (WHERE frequency ILIKE 'POPLATEK PO OBRATU') AS men_unproblematic_issuance_after_transactionl
  FROM f_loan fl
    JOIN d_loan dl ON fl.loan_id = dl.id
    JOIN f_account fa ON fa.account_id = fl.account_id
    JOIN d_account da ON da.id = fa.account_id
    JOIN men ON men.client_id = fa.client_id
  WHERE status IN ('A', 'C')
), men_problematic_loans AS (
  SELECT 
    COUNT(*) FILTER (WHERE frequency ILIKE 'POPLATEK MESICNE') AS men_problematic_monthly_issuance,
    COUNT(*) FILTER (WHERE frequency ILIKE 'POPLATEK TYDNE') AS men_problematic_weekly_issuance,
    COUNT(*) FILTER (WHERE frequency ILIKE 'POPLATEK PO OBRATU') AS men_problematic_issuance_after_transactionl
  FROM f_loan fl
    JOIN d_loan dl ON fl.loan_id = dl.id
    JOIN f_account fa ON fa.account_id = fl.account_id
    JOIN d_account da ON da.id = fa.account_id
    JOIN men ON men.client_id = fa.client_id
  WHERE status IN ('B', 'D')
), women_unproblematic_loans AS (
  SELECT 
    COUNT(*) FILTER (WHERE frequency ILIKE 'POPLATEK MESICNE') AS women_unproblematic_monthly_issuance,
    COUNT(*) FILTER (WHERE frequency ILIKE 'POPLATEK TYDNE') AS women_unproblematic_weekly_issuance,
    COUNT(*) FILTER (WHERE frequency ILIKE 'POPLATEK PO OBRATU') AS women_unproblematic_issuance_after_transactionl
  FROM f_loan fl
    JOIN d_loan dl ON fl.loan_id = dl.id
    JOIN f_account fa ON fa.account_id = fl.account_id
    JOIN d_account da ON da.id = fa.account_id
    JOIN women ON women.client_id = fa.client_id
  WHERE status IN ('A', 'C')
), women_problematic_loans AS (
  SELECT 
    COUNT(*) FILTER (WHERE frequency ILIKE 'POPLATEK MESICNE') AS women_problematic_monthly_issuance,
    COUNT(*) FILTER (WHERE frequency ILIKE 'POPLATEK TYDNE') AS women_problematic_weekly_issuance,
    COUNT(*) FILTER (WHERE frequency ILIKE 'POPLATEK PO OBRATU') AS women_problematic_issuance_after_transactionl
  FROM f_loan fl
    JOIN d_loan dl ON fl.loan_id = dl.id
    JOIN f_account fa ON fa.account_id = fl.account_id
    JOIN d_account da ON da.id = fa.account_id
    JOIN women ON women.client_id = fa.client_id
  WHERE status IN ('B', 'D')
)
SELECT *
FROM men_unproblematic_loans, men_problematic_loans, women_unproblematic_loans, women_problematic_loans;

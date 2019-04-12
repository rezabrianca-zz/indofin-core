SELECT
  company_code,
  company_name
FROM company.active_company_history
WHERE snapshot_at = CURRENT_DATE

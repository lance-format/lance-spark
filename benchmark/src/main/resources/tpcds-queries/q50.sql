SELECT s_store_name, s_company_id, s_street_number, s_street_name, s_street_type, s_suite_number, s_city, s_county, s_state, s_zip,
  SUM(CASE WHEN (sr_returned_date_sk - ss_sold_date_sk <= 30) THEN 1 ELSE 0 END) AS days_30,
  SUM(CASE WHEN (sr_returned_date_sk - ss_sold_date_sk > 30) AND (sr_returned_date_sk - ss_sold_date_sk <= 60) THEN 1 ELSE 0 END) AS days_31_60,
  SUM(CASE WHEN (sr_returned_date_sk - ss_sold_date_sk > 60) AND (sr_returned_date_sk - ss_sold_date_sk <= 90) THEN 1 ELSE 0 END) AS days_61_90,
  SUM(CASE WHEN (sr_returned_date_sk - ss_sold_date_sk > 90) AND (sr_returned_date_sk - ss_sold_date_sk <= 120) THEN 1 ELSE 0 END) AS days_91_120,
  SUM(CASE WHEN (sr_returned_date_sk - ss_sold_date_sk > 120) THEN 1 ELSE 0 END) AS days_over120
FROM store_sales, store_returns, store, date_dim d1, date_dim d2
WHERE d2.d_year = 2001 AND d2.d_moy = 8
  AND ss_ticket_number = sr_ticket_number AND ss_item_sk = sr_item_sk AND ss_sold_date_sk = d1.d_date_sk AND sr_returned_date_sk = d2.d_date_sk AND ss_customer_sk = sr_customer_sk AND ss_store_sk = s_store_sk
GROUP BY s_store_name, s_company_id, s_street_number, s_street_name, s_street_type, s_suite_number, s_city, s_county, s_state, s_zip
ORDER BY s_store_name, s_company_id, s_street_number, s_street_name, s_street_type, s_suite_number, s_city, s_county, s_state, s_zip
LIMIT 100

SELECT SUBSTR(w_warehouse_name, 1, 20), sm_type, web_name,
  SUM(CASE WHEN (ws_ship_date_sk - ws_sold_date_sk <= 30) THEN 1 ELSE 0 END) AS days_30,
  SUM(CASE WHEN (ws_ship_date_sk - ws_sold_date_sk > 30) AND (ws_ship_date_sk - ws_sold_date_sk <= 60) THEN 1 ELSE 0 END) AS days_31_60,
  SUM(CASE WHEN (ws_ship_date_sk - ws_sold_date_sk > 60) AND (ws_ship_date_sk - ws_sold_date_sk <= 90) THEN 1 ELSE 0 END) AS days_61_90,
  SUM(CASE WHEN (ws_ship_date_sk - ws_sold_date_sk > 90) AND (ws_ship_date_sk - ws_sold_date_sk <= 120) THEN 1 ELSE 0 END) AS days_91_120,
  SUM(CASE WHEN (ws_ship_date_sk - ws_sold_date_sk > 120) THEN 1 ELSE 0 END) AS days_over120
FROM web_sales, warehouse, ship_mode, web_site, date_dim
WHERE d_month_seq BETWEEN 1200 AND 1200 + 11
  AND ws_ship_date_sk = d_date_sk AND ws_warehouse_sk = w_warehouse_sk AND ws_ship_mode_sk = sm_ship_mode_sk AND ws_web_site_sk = web_site_sk
GROUP BY SUBSTR(w_warehouse_name, 1, 20), sm_type, web_name
ORDER BY SUBSTR(w_warehouse_name, 1, 20), sm_type, web_name
LIMIT 100

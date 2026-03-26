SELECT SUM(ws_ext_discount_amt) AS excess_discount_amount
FROM web_sales, item, date_dim
WHERE i_manufact_id = 350
  AND i_item_sk = ws_item_sk
  AND d_date BETWEEN CAST('2000-01-27' AS DATE) AND DATE_ADD(CAST('2000-01-27' AS DATE), 90)
  AND d_date_sk = ws_sold_date_sk
  AND ws_ext_discount_amt > (
    SELECT 1.3 * AVG(ws_ext_discount_amt)
    FROM web_sales, date_dim
    WHERE ws_item_sk = i_item_sk
      AND d_date BETWEEN CAST('2000-01-27' AS DATE) AND DATE_ADD(CAST('2000-01-27' AS DATE), 90)
      AND d_date_sk = ws_sold_date_sk
  )
ORDER BY SUM(ws_ext_discount_amt)
LIMIT 100

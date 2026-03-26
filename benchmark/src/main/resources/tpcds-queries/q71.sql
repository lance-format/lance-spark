SELECT i_brand_id brand_id, i_brand brand, t_hour, t_minute,
  SUM(ext_price) ext_price
FROM item,
  (SELECT ws_ext_sales_price AS ext_price, ws_sold_date_sk AS sold_date_sk, ws_item_sk AS sold_item_sk, ws_sold_time_sk AS time_sk FROM web_sales
   UNION ALL
   SELECT cs_ext_sales_price AS ext_price, cs_sold_date_sk AS sold_date_sk, cs_item_sk AS sold_item_sk, cs_sold_time_sk AS time_sk FROM catalog_sales
   UNION ALL
   SELECT ss_ext_sales_price AS ext_price, ss_sold_date_sk AS sold_date_sk, ss_item_sk AS sold_item_sk, ss_sold_time_sk AS time_sk FROM store_sales
  ) tmp, date_dim, time_dim
WHERE sold_date_sk = d_date_sk AND i_item_sk = sold_item_sk AND sold_time_sk = t_time_sk
  AND i_manager_id = 1 AND d_moy = 11 AND d_year = 1999
GROUP BY i_brand, i_brand_id, t_hour, t_minute
ORDER BY ext_price DESC, i_brand_id

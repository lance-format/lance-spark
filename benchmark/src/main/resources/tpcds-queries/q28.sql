SELECT *
FROM (
  SELECT AVG(ss_list_price) B1_LP, COUNT(ss_list_price) B1_CNT, COUNT(DISTINCT ss_list_price) B1_CNTD
  FROM store_sales WHERE ss_quantity BETWEEN 0 AND 5 AND (ss_list_price BETWEEN 8 AND 8 + 10 OR ss_coupon_amt BETWEEN 459 AND 459 + 1000 OR ss_wholesale_cost BETWEEN 57 AND 57 + 20)
) B1,
(
  SELECT AVG(ss_list_price) B2_LP, COUNT(ss_list_price) B2_CNT, COUNT(DISTINCT ss_list_price) B2_CNTD
  FROM store_sales WHERE ss_quantity BETWEEN 6 AND 10 AND (ss_list_price BETWEEN 90 AND 90 + 10 OR ss_coupon_amt BETWEEN 2323 AND 2323 + 1000 OR ss_wholesale_cost BETWEEN 31 AND 31 + 20)
) B2,
(
  SELECT AVG(ss_list_price) B3_LP, COUNT(ss_list_price) B3_CNT, COUNT(DISTINCT ss_list_price) B3_CNTD
  FROM store_sales WHERE ss_quantity BETWEEN 11 AND 15 AND (ss_list_price BETWEEN 142 AND 142 + 10 OR ss_coupon_amt BETWEEN 12214 AND 12214 + 1000 OR ss_wholesale_cost BETWEEN 79 AND 79 + 20)
) B3,
(
  SELECT AVG(ss_list_price) B4_LP, COUNT(ss_list_price) B4_CNT, COUNT(DISTINCT ss_list_price) B4_CNTD
  FROM store_sales WHERE ss_quantity BETWEEN 16 AND 20 AND (ss_list_price BETWEEN 135 AND 135 + 10 OR ss_coupon_amt BETWEEN 6071 AND 6071 + 1000 OR ss_wholesale_cost BETWEEN 38 AND 38 + 20)
) B4,
(
  SELECT AVG(ss_list_price) B5_LP, COUNT(ss_list_price) B5_CNT, COUNT(DISTINCT ss_list_price) B5_CNTD
  FROM store_sales WHERE ss_quantity BETWEEN 21 AND 25 AND (ss_list_price BETWEEN 122 AND 122 + 10 OR ss_coupon_amt BETWEEN 836 AND 836 + 1000 OR ss_wholesale_cost BETWEEN 17 AND 17 + 20)
) B5,
(
  SELECT AVG(ss_list_price) B6_LP, COUNT(ss_list_price) B6_CNT, COUNT(DISTINCT ss_list_price) B6_CNTD
  FROM store_sales WHERE ss_quantity BETWEEN 26 AND 30 AND (ss_list_price BETWEEN 154 AND 154 + 10 OR ss_coupon_amt BETWEEN 7326 AND 7326 + 1000 OR ss_wholesale_cost BETWEEN 7 AND 7 + 20)
) B6
LIMIT 100

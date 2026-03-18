SELECT s_store_name, SUM(ss_net_profit)
FROM store_sales, date_dim, store,
  (SELECT ca_zip
   FROM (
     SELECT SUBSTR(ca_zip, 1, 5) ca_zip
     FROM customer_address
     WHERE SUBSTR(ca_zip, 1, 5) IN ('24128','76232','65084','87816','83926','77556','20548','26231','43848','15126','91137','61265','98294','25782','17920','18426','98235','40081','84093','28577','55565','17183','54601','67897','22752','86284','18376','38607','45200','21756','29741','96765','29561','36119','99386','63792','11108','29437','40111','35006','10385','07840','73134','41407','36509','56438','30668','42045','56726','65338','64528','36275','26810','25168','43781','29085','73239','07068','13727','57834','15751','90226','74683','67100','63775','11901','45111','95762','44442','49245','42042','15850','56172','59420','42614','73396')
     INTERSECT
     SELECT ca_zip
     FROM (
       SELECT SUBSTR(ca_zip, 1, 5) ca_zip, COUNT(*) cnt
       FROM customer_address, customer
       WHERE ca_address_sk = c_current_addr_sk
         AND c_preferred_cust_flag = 'Y'
       GROUP BY ca_zip
       HAVING COUNT(*) > 10
     ) A1
   ) A2
  ) V1
WHERE ss_store_sk = s_store_sk
  AND ss_sold_date_sk = d_date_sk
  AND d_qoy = 2 AND d_year = 1998
  AND (SUBSTR(s_zip, 1, 2) = SUBSTR(V1.ca_zip, 1, 2))
GROUP BY s_store_name
ORDER BY s_store_name
LIMIT 100

WITH inv AS (
  SELECT w_warehouse_name, w_warehouse_sk, i_item_sk, d_moy,
    STDDEV_SAMP(inv_quantity_on_hand) stdev, AVG(inv_quantity_on_hand) mean
  FROM inventory, item, warehouse, date_dim
  WHERE inv_item_sk = i_item_sk AND inv_warehouse_sk = w_warehouse_sk AND inv_date_sk = d_date_sk AND d_year = 2001
  GROUP BY w_warehouse_name, w_warehouse_sk, i_item_sk, d_moy
)
SELECT inv1.w_warehouse_sk, inv1.i_item_sk, inv1.d_moy, inv1.mean, inv1.stdev,
  inv2.w_warehouse_sk, inv2.i_item_sk, inv2.d_moy, inv2.mean, inv2.stdev
FROM inv inv1, inv inv2
WHERE inv1.i_item_sk = inv2.i_item_sk AND inv1.w_warehouse_sk = inv2.w_warehouse_sk
  AND inv1.d_moy = 1 AND inv2.d_moy = 1 + 1
  AND inv1.mean > 0
  AND CASE WHEN inv1.mean > 0 THEN inv1.stdev / inv1.mean ELSE NULL END > 1.5
ORDER BY inv1.w_warehouse_sk, inv1.i_item_sk, inv1.d_moy, inv1.mean, inv1.stdev

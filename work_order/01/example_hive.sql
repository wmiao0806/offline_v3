-- 价格带分桶示例（按单价）价格带 & 支付分区
SELECT
  shop_no,
  product_id,
  stat_hour,
  price_bucket,
  SUM(pay_qty) AS bucket_pay_qty,
  SUM(pay_amount) AS bucket_pay_amount
FROM (
  SELECT
    p.shop_no,
    p.product_id,
    p.stat_hour,
    p.pay_qty,
    p.pay_amount,
    CASE
      WHEN p.pay_amount / p.pay_qty < 50 THEN '0-50'
      WHEN p.pay_amount / p.pay_qty >=50 AND p.pay_amount / p.pay_qty < 100 THEN '50-100'
      WHEN p.pay_amount / p.pay_qty >=100 AND p.pay_amount / p.pay_qty < 300 THEN '100-300'
      WHEN p.pay_amount / p.pay_qty >=300 AND p.pay_amount / p.pay_qty < 1000 THEN '300-1000'
      ELSE '1000+'
    END AS price_bucket
  FROM pay_join p
) t
GROUP BY shop_no, product_id, stat_hour, price_bucket;


SELECT 'ods_logs' as tbl, COUNT(*) FROM ods_logs;
SELECT 'dim_favorite', COUNT(*) FROM dim_favorite;
SELECT 'dim_cart', COUNT(*) FROM dim_cart;
SELECT 'order_info', COUNT(*) FROM order_info;
SELECT 'order_detail', COUNT(*) FROM order_detail;



SELECT product_id, COUNT(1) as pv, COUNT(DISTINCT user_id) as uv
FROM ods_logs_jdbc
WHERE ds = '2025-10-17'
GROUP BY product_id
ORDER BY pv DESC
LIMIT 20;

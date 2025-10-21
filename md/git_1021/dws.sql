-- ===============================
-- 4.1 商品指标宽表 dws_sku_metrics
-- ===============================
DROP TABLE IF EXISTS dws_sku_metrics;

CREATE TABLE dws_sku_metrics
(
    store_no       STRING,
    product_id     STRING,
    uv             BIGINT, -- 商品访客数
    pv_cnt         BIGINT, -- 商品浏览量
    favor_user_cnt BIGINT, -- 收藏人数
    cart_user_cnt  BIGINT, -- 加购人数
    cart_quantity  BIGINT, -- 加购件数
    pay_user_cnt   BIGINT, -- 支付买家数
    pay_quantity   BIGINT, -- 支付件数
    pay_amount DOUBLE      -- 支付金额
) PARTITIONED BY (ds STRING)
;

-- ===============================
-- 插入汇总指标数据
-- ===============================
INSERT OVERWRITE TABLE dws_sku_metrics PARTITION(ds='20251020')
SELECT
    pv.store_no,
    pv.product_id,
    COUNT(DISTINCT pv.userid)                      AS uv,
    COUNT(*)                                       AS pv_cnt,
    COUNT(DISTINCT f.user_id)                      AS favor_user_cnt,  -- 改了这里
    COUNT(DISTINCT c.userid)                       AS cart_user_cnt,
    SUM(CAST(c.quantity AS BIGINT))                AS cart_quantity,
    COUNT(DISTINCT o.user_id)                      AS pay_user_cnt,
    SUM(CAST(o.quantity AS BIGINT))                AS pay_quantity,
    SUM(CAST(o.price AS DOUBLE) * CAST(o.quantity AS DOUBLE)) AS pay_amount
FROM dwd_sku_pv pv
LEFT JOIN realtime_v1_ods_favor_info f
  ON pv.product_id = f.sku_id                     -- 删除 f.store_no
  AND pv.ds = f.ds
LEFT JOIN dwd_sku_cart c
  ON pv.product_id = c.product_id AND pv.store_no = c.store_no AND pv.ds = c.ds
LEFT JOIN dwd_sku_order o
  ON pv.product_id = o.product_id AND pv.store_no = o.store_no AND pv.ds = o.ds
WHERE pv.ds = '20251020'
GROUP BY pv.store_no, pv.product_id;



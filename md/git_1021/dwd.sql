-- ====================================================
-- DWD 层建表 + 插入数据（时间字段改为 STRING）
-- ====================================================

-- 3.1 商品访问明细 dwd_sku_pv
drop table IF EXISTS dwd_sku_pv;
CREATE TABLE IF NOT EXISTS dwd_sku_pv (
    store_no STRING,
    product_id STRING,
    userid STRING,
    event_time STRING
)
PARTITIONED BY (ds STRING)
;
INSERT OVERWRITE TABLE dwd_sku_pv PARTITION(ds='20251020')
SELECT
    get_json_object(log, '$.store_no') AS store_no,
    get_json_object(log, '$.product_id') AS product_id,
    get_json_object(log, '$.userid') AS userid,
    get_json_object(log, '$.event_time') AS event_time
FROM realtime_v1_ods_z_log
WHERE ds='20251020'  -- 指定分区
  AND get_json_object(log, '$.event_type')='pv';

SET odps.sql.allow.fullscan=true;
select *
from dwd_sku_pv;

-- INSERT OVERWRITE TABLE dwd_sku_pv PARTITION(ds)
-- SELECT
--     store_no,                 -- 店铺编号
--     product_id,               -- 商品ID
--     userid,                   -- 用户ID
--     CAST(event_time AS STRING) AS event_time,  -- 时间字段改为字符串
--     ds
-- FROM realtime_v1_ods_activity_info
-- WHERE event_type='pv';       -- pv 访问事件，根据你实际字段调整

-- 3.2 商品加购明细 dwd_sku_cart
drop table IF EXISTS dwd_sku_cart;
CREATE TABLE IF NOT EXISTS dwd_sku_cart (
    store_no STRING,
    product_id STRING,
    userid STRING,
    cart_time STRING,
    quantity INT
)PARTITIONED BY (ds STRING)
;
INSERT OVERWRITE TABLE dwd_sku_cart PARTITION(ds='20251020')
SELECT
    NULL AS store_no,                     -- 没有 store_no，先用 NULL
    sku_id AS product_id,
    user_id AS userid,
    create_time AS cart_time,
    CAST(sku_num AS INT) AS quantity      -- 显式类型转换
FROM realtime_v1_ods_cart_info
WHERE ds='20251020';

-- INSERT OVERWRITE TABLE dwd_sku_cart PARTITION(ds='20251020')
-- SELECT
--     NULL AS store_no,                     -- 没有 store_no 字段，可以先用 NULL
--     sku_id AS product_id,
--     user_id AS userid,
--     create_time AS cart_time,
--     sku_num AS quantity
-- FROM realtime_v1_ods_cart_info
-- WHERE ds='20251020';


-- 3.3 订单支付明细 dwd_sku_order
drop table IF EXISTS dwd_sku_order;
CREATE TABLE IF NOT EXISTS dwd_sku_order (
    order_id STRING,
    product_id STRING,
    user_id STRING,
    store_no STRING,
    quantity INT,
    price DOUBLE,
    pay_time STRING
)
PARTITIONED BY (ds STRING);
INSERT OVERWRITE TABLE dwd_sku_order PARTITION(ds='20251020')
SELECT
    d.order_id,                                -- 订单ID
    d.sku_id      AS product_id,               -- 商品ID
    o.user_id     AS userid,                   -- 用户ID
    NULL          AS store_no,                 -- store_no 源表无此字段
    CAST(d.sku_num AS INT) AS quantity,        -- 数量
    CAST(d.order_price AS DOUBLE) AS price,    -- 手动类型转换 ✅
    CAST(o.create_time AS STRING) AS pay_time  -- 支付时间
FROM realtime_v1_ods_order_detail d
JOIN realtime_v1_ods_order_info o
  ON d.order_id = o.id
WHERE o.order_status = '1'
  AND o.ds = '20251020'
  AND d.ds = '20251020';
select * from realtime_v1_ods_order_detail where ds='20251019';
select * from realtime_v1_ods_base_category3 where ds='20251019';
select * from dwd_sku_order where ds='20251019';
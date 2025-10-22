-- DWS层：商品核心指标日聚合表
DROP TABLE IF EXISTS dws_agg_sku_daily;
CREATE TABLE dws_agg_sku_daily (
    stat_date STRING,
    sku_id BIGINT,
    sku_name string,
    category3_id BIGINT,
    category3_name string,
    tm_id BIGINT,
    tm_name string,
    -- 访问指标
    visitor_count BIGINT,
    page_view_count BIGINT,
    -- 互动指标
    favor_count BIGINT,
    cart_count BIGINT,
    cart_user_count BIGINT,
    -- 交易指标
    order_user_count BIGINT,
    order_sku_count BIGINT,
    order_amount DECIMAL(16,2),
    payment_user_count BIGINT,
    payment_sku_count BIGINT,
    payment_amount DECIMAL(16,2),
    -- 转化率指标
    visitor_to_favor_rate DECIMAL(10,4),
    visitor_to_cart_rate DECIMAL(10,4),
    visitor_to_order_rate DECIMAL(10,4),
    visitor_to_payment_rate DECIMAL(10,4),
    -- 价值指标
    avg_order_price DECIMAL(10,2),
    visitor_avg_value DECIMAL(10,2),
    create_time STRING,
    operate_time STRING
) COMMENT '商品核心指标日聚合表'
PARTITIONED BY (ds STRING);

-- 插入数据
INSERT OVERWRITE TABLE dws_agg_sku_daily PARTITION(ds)
SELECT
    TO_CHAR(TO_DATE(bd.behavior_time, 'yyyy-mm-dd hh:mi:ss'), 'yyyy-mm-dd') as stat_date,
    bd.sku_id as sku_id,
    dsf.sku_name as sku_name,
    dsf.category3_id as category3_id,
    dsf.category3_name as category3_name,
    dsf.tm_id as tm_id,
    dsf.tm_name as tm_name,
    COUNT(DISTINCT bd.user_id) as visitor_count,
    COUNT(*) as page_view_count,
    COUNT(DISTINCT CASE WHEN bd.behavior_type = 'favor' THEN bd.user_id END) as favor_count,
    SUM(CASE WHEN bd.behavior_type = 'cart' THEN bd.cart_num ELSE 0 END) as cart_count,
    COUNT(DISTINCT CASE WHEN bd.behavior_type = 'cart' THEN bd.user_id END) as cart_user_count,
    COUNT(DISTINCT CASE WHEN bd.behavior_type = 'order' THEN bd.user_id END) as order_user_count,
    SUM(CASE WHEN bd.behavior_type = 'order' THEN bd.sku_num ELSE 0 END) as order_sku_count,
    CAST(SUM(CASE WHEN bd.behavior_type = 'order' THEN bd.total_amount ELSE 0 END) AS DECIMAL(16,2)) as order_amount,
    COUNT(DISTINCT td.user_id) as payment_user_count,
    SUM(td.sku_num) as payment_sku_count,
    CAST(SUM(td.total_amount) AS DECIMAL(16,2)) as payment_amount,
    ROUND(COUNT(DISTINCT CASE WHEN bd.behavior_type = 'favor' THEN bd.user_id END) * 1.0 /
          NULLIF(COUNT(DISTINCT bd.user_id), 0), 4) as visitor_to_favor_rate,
    ROUND(COUNT(DISTINCT CASE WHEN bd.behavior_type = 'cart' THEN bd.user_id END) * 1.0 /
          NULLIF(COUNT(DISTINCT bd.user_id), 0), 4) as visitor_to_cart_rate,
    ROUND(COUNT(DISTINCT CASE WHEN bd.behavior_type = 'order' THEN bd.user_id END) * 1.0 /
          NULLIF(COUNT(DISTINCT bd.user_id), 0), 4) as visitor_to_order_rate,
    ROUND(COUNT(DISTINCT td.user_id) * 1.0 / NULLIF(COUNT(DISTINCT bd.user_id), 0), 4) as visitor_to_payment_rate,
    CAST(ROUND(SUM(td.total_amount) / NULLIF(COUNT(DISTINCT td.user_id), 0), 2) AS DECIMAL(10,2)) as avg_order_price,
    CAST(ROUND(SUM(td.total_amount) / NULLIF(COUNT(DISTINCT bd.user_id), 0), 2) AS DECIMAL(10,2)) as visitor_avg_value,
    TO_CHAR(GETDATE(), 'yyyy-mm-dd hh:mi:ss') as create_time,
    TO_CHAR(GETDATE(), 'yyyy-mm-dd hh:mi:ss') as operate_time,
    TO_CHAR(TO_DATE(bd.behavior_time, 'yyyy-mm-dd hh:mi:ss'), 'yyyy-mm-dd') as ds
FROM dwd_fact_behavior_detail bd
LEFT JOIN dwd_fact_trade_detail td ON bd.sku_id = td.sku_id
    AND TO_CHAR(TO_DATE(bd.behavior_time, 'yyyy-mm-dd hh:mi:ss'), 'yyyy-mm-dd') = TO_CHAR(TO_DATE(td.payment_time, 'yyyy-mm-dd hh:mi:ss'), 'yyyy-mm-dd')
    AND td.ds = '20251019'
LEFT JOIN dwd_dim_sku_full dsf ON bd.sku_id = dsf.sku_id
    AND dsf.ds = '20251019'
WHERE bd.ds = '20251019'
GROUP BY
    TO_CHAR(TO_DATE(bd.behavior_time, 'yyyy-mm-dd hh:mi:ss'), 'yyyy-mm-dd'),
    bd.sku_id,
    dsf.sku_name,
    dsf.category3_id,
    dsf.category3_name,
    dsf.tm_id,
    dsf.tm_name;

-- 验证数据
SELECT COUNT(*) FROM dws_agg_sku_daily WHERE ds = '2025-09-22';

-- DWS层：分类核心指标日聚合表
DROP TABLE IF EXISTS dws_agg_category_daily;
CREATE TABLE if not EXISTS dws_agg_category_daily (
    stat_date STRING,
    category3_id BIGINT,
    category3_name string,
    category2_id BIGINT,
    category2_name string,
    category1_id BIGINT,
    category1_name string,
    -- 核心指标
    visitor_count BIGINT,
    sku_with_visitor_count BIGINT,
    payment_user_count BIGINT,
    payment_sku_count BIGINT,
    payment_amount DECIMAL(16,2),
    sku_with_payment_count BIGINT,
    payment_conversion_rate DECIMAL(10,4),
    avg_order_price DECIMAL(10,2),
    create_time STRING,
    operate_time STRING
) COMMENT '分类核心指标日聚合表'
PARTITIONED BY (ds STRING);

-- 插入数据
-- DWS层：分类核心指标日聚合表
INSERT OVERWRITE TABLE dws_agg_category_daily PARTITION(ds)
SELECT
    dsd.stat_date as stat_date,
    dsd.category3_id as category3_id,
    dsd.category3_name as category3_name,
    c2.id as category2_id,
    c2.name as category2_name,
    c1.id as category1_id,
    c1.name as category1_name,
    SUM(dsd.visitor_count) as visitor_count,
    COUNT(DISTINCT dsd.sku_id) as sku_with_visitor_count,
    SUM(dsd.payment_user_count) as payment_user_count,
    SUM(dsd.payment_sku_count) as payment_sku_count,
    -- 添加显式类型转换
    CAST(SUM(dsd.payment_amount) AS DECIMAL(16,2)) as payment_amount,
    COUNT(DISTINCT CASE WHEN dsd.payment_user_count > 0 THEN dsd.sku_id END) as sku_with_payment_count,
    ROUND(SUM(dsd.payment_user_count) * 1.0 / NULLIF(SUM(dsd.visitor_count), 0), 4) as payment_conversion_rate,
    -- 添加显式类型转换
    CAST(ROUND(SUM(dsd.payment_amount) / NULLIF(SUM(dsd.payment_user_count), 0), 2) AS DECIMAL(10,2)) as avg_order_price,
    TO_CHAR(GETDATE(), 'yyyy-mm-dd hh:mi:ss') as create_time,
    TO_CHAR(GETDATE(), 'yyyy-mm-dd hh:mi:ss') as operate_time,
    dsd.stat_date as ds
FROM dws_agg_sku_daily dsd
LEFT JOIN realtime_v1_ods_base_category3 c3 ON dsd.category3_id = c3.id
LEFT JOIN realtime_v1_ods_base_category2 c2 ON c3.category2_id = c2.id
LEFT JOIN realtime_v1_ods_base_category1 c1 ON c2.category1_id = c1.id
WHERE dsd.ds = '2025-09-22'
GROUP BY dsd.stat_date, dsd.category3_id, dsd.category3_name, c2.id, c2.name, c1.id, c1.name;

-- DWS层：品牌核心指标日聚合表
-- DWS层：品牌核心指标日聚合表
DROP TABLE IF EXISTS dws_agg_trademark_daily;
CREATE TABLE dws_agg_trademark_daily (
    stat_date STRING,
    tm_id BIGINT,
    tm_name string,
    -- 核心指标
    visitor_count BIGINT,
    sku_with_visitor_count BIGINT,
    payment_user_count BIGINT,
    payment_sku_count BIGINT,
    payment_amount DECIMAL(16,2),
    sku_with_payment_count BIGINT,
    payment_conversion_rate DECIMAL(10,4),
    avg_order_price DECIMAL(10,2),
    create_time STRING,
    operate_time STRING
) COMMENT '品牌核心指标日聚合表'
PARTITIONED BY (ds STRING);

-- 插入数据
INSERT OVERWRITE TABLE dws_agg_trademark_daily PARTITION(ds)
SELECT
    dsd.stat_date as stat_date,
    dsd.tm_id as tm_id,
    dsd.tm_name as tm_name,
    SUM(dsd.visitor_count) as visitor_count,
    COUNT(DISTINCT dsd.sku_id) as sku_with_visitor_count,
    SUM(dsd.payment_user_count) as payment_user_count,
    SUM(dsd.payment_sku_count) as payment_sku_count,
    -- 添加显式类型转换
    CAST(SUM(dsd.payment_amount) AS DECIMAL(16,2)) as payment_amount,
    COUNT(DISTINCT CASE WHEN dsd.payment_user_count > 0 THEN dsd.sku_id END) as sku_with_payment_count,
    ROUND(SUM(dsd.payment_user_count) * 1.0 / NULLIF(SUM(dsd.visitor_count), 0), 4) as payment_conversion_rate,
    -- 添加显式类型转换
    CAST(ROUND(SUM(dsd.payment_amount) / NULLIF(SUM(dsd.payment_user_count), 0), 2) AS DECIMAL(10,2)) as avg_order_price,
    TO_CHAR(GETDATE(), 'yyyy-mm-dd hh:mi:ss') as create_time,
    TO_CHAR(GETDATE(), 'yyyy-mm-dd hh:mi:ss') as operate_time,
    dsd.stat_date as ds
FROM dws_agg_sku_daily dsd
WHERE dsd.ds = '2025-09-22'
GROUP BY dsd.stat_date, dsd.tm_id, dsd.tm_name;
-- INSERT OVERWRITE TABLE dws_agg_trademark_daily PARTITION(ds)
-- SELECT
--     dsd.stat_date as stat_date,
--     dsd.tm_id as tm_id,
--     dsd.tm_name as tm_name,
--     SUM(dsd.visitor_count) as visitor_count,
--     COUNT(DISTINCT dsd.sku_id) as sku_with_visitor_count,
--     SUM(dsd.payment_user_count) as payment_user_count,
--     SUM(dsd.payment_sku_count) as payment_sku_count,
--     SUM(dsd.payment_amount) as payment_amount,
--     COUNT(DISTINCT CASE WHEN dsd.payment_user_count > 0 THEN dsd.sku_id END) as sku_with_payment_count,
--     ROUND(SUM(dsd.payment_user_count) * 1.0 / NULLIF(SUM(dsd.visitor_count), 0), 4) as payment_conversion_rate,
--     ROUND(SUM(dsd.payment_amount) / NULLIF(SUM(dsd.payment_user_count), 0), 2) as avg_order_price,
--     TO_CHAR(GETDATE(), 'yyyy-mm-dd hh:mi:ss') as create_time,
--     TO_CHAR(GETDATE(), 'yyyy-mm-dd hh:mi:ss') as operate_time,
--     dsd.stat_date as ds
-- FROM dws_agg_sku_daily dsd
-- WHERE dsd.ds = '2025-09-22'
-- GROUP BY dsd.stat_date, dsd.tm_id, dsd.tm_name;

-- DWS层：用户行为聚合表
INSERT OVERWRITE TABLE dws_agg_user_behavior_daily PARTITION(ds)
SELECT
    TO_CHAR(TO_DATE(bd.behavior_time, 'yyyy-mm-dd hh:mi:ss'), 'yyyy-mm-dd') as stat_date,
    bd.user_id as user_id,
    COUNT(*) as page_view_count,
    COUNT(CASE WHEN bd.behavior_type = 'favor' THEN 1 END) as favor_count,
    COUNT(CASE WHEN bd.behavior_type = 'cart' THEN 1 END) as cart_count,
    COUNT(CASE WHEN bd.behavior_type = 'order' THEN 1 END) as order_count,
    COUNT(DISTINCT td.order_id) as payment_count,
    -- 添加显式类型转换
    CAST(SUM(CASE WHEN bd.behavior_type = 'order' THEN bd.total_amount ELSE 0 END) AS DECIMAL(16,2)) as total_order_amount,
    -- 添加显式类型转换
    CAST(SUM(td.total_amount) AS DECIMAL(16,2)) as total_payment_amount,
    1 as active_days,
    MAX(bd.behavior_time) as last_behavior_time,
    TO_CHAR(GETDATE(), 'yyyy-mm-dd hh:mi:ss') as create_time,
    TO_CHAR(GETDATE(), 'yyyy-mm-dd hh:mi:ss') as operate_time,
    TO_CHAR(TO_DATE(bd.behavior_time, 'yyyy-mm-dd hh:mi:ss'), 'yyyy-mm-dd') as ds
FROM dwd_fact_behavior_detail bd
LEFT JOIN dwd_fact_trade_detail td ON bd.user_id = td.user_id AND TO_CHAR(TO_DATE(bd.behavior_time, 'yyyy-mm-dd hh:mi:ss'), 'yyyy-mm-dd') = TO_CHAR(TO_DATE(td.payment_time, 'yyyy-mm-dd hh:mi:ss'), 'yyyy-mm-dd')
WHERE TO_CHAR(TO_DATE(bd.behavior_time, 'yyyy-mm-dd hh:mi:ss'), 'yyyy-mm-dd') = '2025-09-22'
GROUP BY
    TO_CHAR(TO_DATE(bd.behavior_time, 'yyyy-mm-dd hh:mi:ss'), 'yyyy-mm-dd'),
    bd.user_id;
-- 验证数据
SELECT COUNT(*) FROM dws_agg_category_daily WHERE ds = '2025-09-22';

-- 验证数据
SELECT COUNT(*) FROM dws_agg_trademark_daily WHERE ds = '2025-09-22';

-- DWS层：用户行为聚合表
DROP TABLE IF EXISTS dws_agg_user_behavior_daily;
CREATE TABLE dws_agg_user_behavior_daily (
    stat_date STRING,
    user_id BIGINT,
    -- 行为指标
    page_view_count BIGINT,
    favor_count BIGINT,
    cart_count BIGINT,
    order_count BIGINT,
    payment_count BIGINT,
    -- 金额指标
    total_order_amount DECIMAL(16,2),
    total_payment_amount DECIMAL(16,2),
    -- 活跃度指标
    active_days INT,
    last_behavior_time STRING,
    create_time STRING,
    operate_time STRING
) COMMENT '用户行为聚合表'
PARTITIONED BY (ds STRING);

-- 插入数据
INSERT OVERWRITE TABLE dws_agg_user_behavior_daily PARTITION(ds)
SELECT
    TO_CHAR(TO_DATE(bd.behavior_time, 'yyyy-mm-dd hh:mi:ss'), 'yyyy-mm-dd') as stat_date,
    bd.user_id as user_id,
    COUNT(*) as page_view_count,
    COUNT(CASE WHEN bd.behavior_type = 'favor' THEN 1 END) as favor_count,
    COUNT(CASE WHEN bd.behavior_type = 'cart' THEN 1 END) as cart_count,
    COUNT(CASE WHEN bd.behavior_type = 'order' THEN 1 END) as order_count,
    COUNT(DISTINCT td.order_id) as payment_count,
    -- 添加显式类型转换
    CAST(SUM(CASE WHEN bd.behavior_type = 'order' THEN bd.total_amount ELSE 0 END) AS DECIMAL(16,2)) as total_order_amount,
    -- 添加显式类型转换
    CAST(SUM(td.total_amount) AS DECIMAL(16,2)) as total_payment_amount,
    1 as active_days,
    MAX(bd.behavior_time) as last_behavior_time,
    TO_CHAR(GETDATE(), 'yyyy-mm-dd hh:mi:ss') as create_time,
    TO_CHAR(GETDATE(), 'yyyy-mm-dd hh:mi:ss') as operate_time,
    TO_CHAR(TO_DATE(bd.behavior_time, 'yyyy-mm-dd hh:mi:ss'), 'yyyy-mm-dd') as ds
FROM dwd_fact_behavior_detail bd
LEFT JOIN dwd_fact_trade_detail td ON bd.user_id = td.user_id
    AND TO_CHAR(TO_DATE(bd.behavior_time, 'yyyy-mm-dd hh:mi:ss'), 'yyyy-mm-dd') = TO_CHAR(TO_DATE(td.payment_time, 'yyyy-mm-dd hh:mi:ss'), 'yyyy-mm-dd')
    AND td.ds = '20251019'  -- 添加交易表的分区谓词
WHERE bd.ds = '20251019'  -- 添加行为表的分区谓词
  AND TO_CHAR(TO_DATE(bd.behavior_time, 'yyyy-mm-dd hh:mi:ss'), 'yyyy-mm-dd') = '2025-09-22'
GROUP BY
    TO_CHAR(TO_DATE(bd.behavior_time, 'yyyy-mm-dd hh:mi:ss'), 'yyyy-mm-dd'),
    bd.user_id;

-- 验证数据
SELECT COUNT(*) FROM dws_agg_user_behavior_daily WHERE ds = '2025-09-22';
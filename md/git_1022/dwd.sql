-- DWD层：商品维度宽表
CREATE TABLE dwd_dim_sku_full (
    sku_id BIGINT,
    sku_name VARCHAR(255),
    spu_id BIGINT,
    spu_name VARCHAR(255),
    tm_id BIGINT,
    tm_name VARCHAR(255),
    category3_id BIGINT,
    category3_name VARCHAR(255),
    category2_id BIGINT,
    category2_name VARCHAR(255),
    category1_id BIGINT,
    category1_name VARCHAR(255),
    price DECIMAL(16,2),
    create_time string,
    operate_time string
) PARTITIONED BY (ds STRING);

-- 插入数据
-- 插入数据到商品维度宽表（指定分区）
-- 插入数据到商品维度宽表（使用固定分区值）
INSERT OVERWRITE TABLE dwd_dim_sku_full PARTITION (ds = '20251019')
SELECT
    s.id as sku_id,
    s.sku_name as sku_name,
    s.spu_id as spu_id,
    p.spu_name as spu_name,
    s.tm_id as tm_id,
    t.tm_name as tm_name,
    s.category3_id as category3_id,
    c3.name as category3_name,
    c2.id as category2_id,
    c2.name as category2_name,
    c1.id as category1_id,
    c1.name as category1_name,
    -- 显式转换price字段类型
    CAST(s.price AS DECIMAL(16,2)) as price,
    -- 显式转换时间字段类型
    CAST(s.create_time AS string) as create_time,
    CAST(s.operate_time AS string) as operate_time
FROM realtime_v1_ods_sku_info s
LEFT JOIN realtime_v1_ods_spu_info p ON s.spu_id = p.id AND s.ds = p.ds
LEFT JOIN realtime_v1_ods_base_trademark t ON s.tm_id = t.id AND s.ds = t.ds
LEFT JOIN realtime_v1_ods_base_category3 c3 ON s.category3_id = c3.id AND s.ds = c3.ds
LEFT JOIN realtime_v1_ods_base_category2 c2 ON c3.category2_id = c2.id AND s.ds = c2.ds
LEFT JOIN realtime_v1_ods_base_category1 c1 ON c2.category1_id = c1.id AND s.ds = c1.ds
WHERE s.ds = '20251019';
-- 验证数据
SELECT COUNT(*) FROM dwd_dim_sku_full where ds = '20251019';

-- DWD层：订单交易明细表
DROP TABLE IF EXISTS dwd_fact_order_trade_detail;
CREATE TABLE dwd_fact_order_trade_detail (
    order_id BIGINT COMMENT '订单ID',
    user_id BIGINT COMMENT '用户ID',
    sku_id BIGINT COMMENT '商品SKU ID',
    order_price DECIMAL(10,2) COMMENT '订单价格',
    sku_num INT COMMENT '商品数量',
    total_amount DECIMAL(16,2) COMMENT '总金额',
    activity_amount DECIMAL(16,2) COMMENT '活动优惠金额',
    coupon_amount DECIMAL(16,2) COMMENT '优惠券金额',
    order_status VARCHAR(20) COMMENT '订单状态',
    province_id BIGINT COMMENT '省份ID',
    create_time STRING COMMENT '创建时间',
    operate_time STRING COMMENT '修改时间'
) PARTITIONED BY (ds STRING);

-- 插入数据（指定分区）
-- DWD层：订单交易明细表
INSERT OVERWRITE TABLE dwd_fact_order_trade_detail PARTITION (ds = '20251020')
SELECT
    od.order_id,
    oi.user_id,
    od.sku_id,
    CAST(od.order_price AS DECIMAL(10,2)) as order_price,
    -- 显式转换sku_num字段类型
    CAST(od.sku_num AS INT) as sku_num,
    CAST(od.split_total_amount AS DECIMAL(16,2)) as total_amount,
    -- 修正字段名，使用order_detail表中的字段
    CAST(COALESCE(od.split_activity_amount, 0) AS DECIMAL(16,2)) as activity_amount,
    CAST(COALESCE(od.split_coupon_amount, 0) AS DECIMAL(16,2)) as coupon_amount,
    oi.order_status as order_status,
    oi.province_id,
    od.create_time,
    COALESCE(od.operate_time, '2025-10-20 00:00:00') as operate_time
FROM realtime_v1_ods_order_detail od
LEFT JOIN realtime_v1_ods_order_info oi ON od.order_id = oi.id AND od.ds = oi.ds
-- 移除不存在的关联表
WHERE od.ds = '20251020' AND od.create_time IS NOT NULL;

-- 验证数据
SELECT COUNT(*) as total_records FROM dwd_fact_order_trade_detail WHERE ds = '20251020';


-- DWD层：用户行为事实表
DROP TABLE IF EXISTS dwd_fact_behavior_detail;
CREATE TABLE dwd_fact_behavior_detail (
    user_id BIGINT COMMENT '用户ID',
    sku_id BIGINT COMMENT '商品SKU ID',
    spu_id BIGINT COMMENT '商品SPU ID',
    behavior_type VARCHAR(20) COMMENT '行为类型(cart-加购,favor-收藏,comment-评价)',
    behavior_time STRING COMMENT '行为时间',
    order_id BIGINT COMMENT '订单ID',
    order_price DECIMAL(10,2) COMMENT '订单价格',
    sku_num INT COMMENT '商品数量',
    total_amount DECIMAL(16,2) COMMENT '总金额',
    cart_num INT COMMENT '加购数量',
    is_cancel VARCHAR(1) COMMENT '是否取消',
    appraise VARCHAR(10) COMMENT '评价等级',
    comment_txt STRING COMMENT '评价内容',
    create_time STRING COMMENT '创建时间',
    operate_time STRING COMMENT '操作时间'
) PARTITIONED BY (ds STRING);

-- 插入数据（合并购物车、收藏、评价行为）
INSERT OVERWRITE TABLE dwd_fact_behavior_detail PARTITION (ds)
SELECT
    user_id,
    sku_id,
    spu_id,
    behavior_type,
    behavior_time,
    order_id,
    order_price,
    sku_num,
    total_amount,
    cart_num,
    is_cancel,
    appraise,
    comment_txt,
    create_time,
    operate_time,
    ds
FROM (
    -- 购物车行为
    SELECT
        -- 直接转换user_id
        CAST(user_id AS BIGINT) as user_id,
        sku_id,
        NULL as spu_id,
        'cart' as behavior_type,
        create_time as behavior_time,
        NULL as order_id,
        CAST(cart_price AS DECIMAL(10,2)) as order_price,
        CAST(sku_num AS INT) as sku_num,
        CAST(cart_price * sku_num AS DECIMAL(16,2)) as total_amount,
        CAST(sku_num AS INT) as cart_num,
        NULL as is_cancel,
        NULL as appraise,
        NULL as comment_txt,
        create_time,
        operate_time,
        ds
    FROM realtime_v1_ods_cart_info
    WHERE ds = '20251019' AND create_time IS NOT NULL

    UNION ALL

    -- 收藏行为
    SELECT
        user_id,
        sku_id,
        spu_id,
        'favor' as behavior_type,
        create_time as behavior_time,
        NULL as order_id,
        NULL as order_price,
        NULL as sku_num,
        NULL as total_amount,
        NULL as cart_num,
        is_cancel,
        NULL as appraise,
        NULL as comment_txt,
        create_time,
        operate_time,
        ds
    FROM realtime_v1_ods_favor_info
    WHERE ds = '20251019' AND create_time IS NOT NULL

    UNION ALL

    -- 评价行为
    SELECT
        user_id,
        sku_id,
        spu_id,
        'comment' as behavior_type,
        create_time as behavior_time,
        order_id,
        NULL as order_price,
        NULL as sku_num,
        NULL as total_amount,
        NULL as cart_num,
        NULL as is_cancel,
        appraise,
        comment_txt,
        create_time,
        operate_time,
        ds
    FROM realtime_v1_ods_comment_info
    WHERE ds = '20251019' AND create_time IS NOT NULL
) t;


-- DWD层：交易事实宽表
DROP TABLE IF EXISTS dwd_fact_trade_detail;
CREATE TABLE dwd_fact_trade_detail (
    order_id BIGINT,
    user_id BIGINT,
    sku_id BIGINT,
    sku_name VARCHAR(255),
    order_price DECIMAL(10,2),
    sku_num INT,
    total_amount DECIMAL(16,2),
    activity_amount DECIMAL(16,2),
    coupon_amount DECIMAL(16,2),
    order_status VARCHAR(20),
    order_status_name VARCHAR(50),
    category3_id BIGINT,
    category3_name VARCHAR(255),
    tm_id BIGINT,
    tm_name VARCHAR(255),
    payment_time STRING,
    create_time STRING,
    operate_time STRING
) PARTITIONED BY (ds STRING);

-- 插入数据
INSERT OVERWRITE TABLE dwd_fact_trade_detail PARTITION (ds = '20251019')
SELECT
    od.order_id as order_id,
    oi.user_id as user_id,
    od.sku_id as sku_id,
    dsf.sku_name as sku_name,
    CAST(od.order_price AS DECIMAL(10,2)) as order_price,
    -- 显式转换sku_num为INT
    CAST(od.sku_num AS INT) as sku_num,
    CAST(od.split_total_amount A S DECIMAL(16,2)) as total_amount,
    -- 直接从order_detail表获取活动金额
    CAST(COALESCE(od.split_activity_amount, 0) AS DECIMAL(16,2)) as activity_amount,
    -- 直接从order_detail表获取优惠券金额
    CAST(COALESCE(od.split_coupon_amount, 0) AS DECIMAL(16,2)) as coupon_amount,
    oi.order_status as order_status,
    bd.dic_name as order_status_name,
    dsf.category3_id as category3_id,
    dsf.category3_name as category3_name,
    dsf.tm_id as tm_id,
    dsf.tm_name as tm_name,
    -- 使用支付表的callback_time作为支付时间，如果没有则使用订单操作时间
    COALESCE(pi.callback_time, oi.operate_time) as payment_time,
    od.create_time as create_time,
    COALESCE(od.operate_time, '2025-10-19 00:00:00') as operate_time
FROM realtime_v1_ods_order_detail od
LEFT JOIN realtime_v1_ods_order_info oi ON od.order_id = oi.id AND od.ds = oi.ds
LEFT JOIN realtime_v1_ods_base_dic bd ON oi.order_status = bd.dic_code AND od.ds = bd.ds
LEFT JOIN dwd_dim_sku_full dsf ON od.sku_id = dsf.sku_id AND dsf.ds = '20251019'
-- 移除不存在的关联表，因为金额字段已经在order_detail表中
LEFT JOIN realtime_v1_ods_payment_info pi ON od.order_id = pi.order_id AND od.ds = pi.ds
WHERE od.ds = '20251019' AND od.create_time IS NOT NULL;
-- 验证数据
SELECT COUNT(*) FROM dwd_fact_trade_detail WHERE ds = '20251019';


-- DWD层用户行为宽表
DROP TABLE IF EXISTS dwd_user_behavior_wide;
CREATE TABLE dwd_user_behavior_wide (
    user_id BIGINT COMMENT '用户ID',
    sku_id BIGINT COMMENT '商品SKU ID',
    spu_id BIGINT COMMENT '商品SPU ID',
    behavior_type VARCHAR(20) COMMENT '行为类型(cart-加购,favor-收藏,comment-评价)',
    behavior_time STRING COMMENT '行为时间',
    cart_num INT COMMENT '加购数量',
    is_cancel VARCHAR(1) COMMENT '是否取消',
    appraise VARCHAR(10) COMMENT '评价等级',
    comment_txt STRING COMMENT '评价内容',
    create_time STRING COMMENT '创建时间'
) COMMENT '用户行为宽表'
PARTITIONED BY (ds STRING);

-- 插入数据
INSERT OVERWRITE TABLE dwd_user_behavior_wide PARTITION (ds)
SELECT
    user_id,
    sku_id,
    spu_id,
    behavior_type,
    behavior_time,
    cart_num,
    is_cancel,
    appraise,
    comment_txt,
    create_time,
    ds
FROM dwd_fact_behavior_detail
WHERE ds = '20251021';


select *
from dwd_user_behavior_wide where ds='20251021';
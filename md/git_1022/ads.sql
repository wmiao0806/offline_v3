-- 1. 商品流量指标表（按商品+日期分区）

DROP TABLE IF EXISTS ads_sku_traffic_day;
CREATE TABLE ads_sku_traffic_day (
    sku_id STRING NOT NULL COMMENT '商品SKU ID',
    sku_name STRING COMMENT '商品名称',
    shop_id STRING COMMENT '店铺ID',
    shop_name STRING COMMENT '店铺名称',
    tm_id STRING COMMENT '品牌ID',
    tm_name STRING COMMENT '品牌名称',
    category3_id STRING COMMENT '三级分类ID',
    category3_name STRING COMMENT '三级分类名称',
    total_uv BIGINT DEFAULT 0 COMMENT '总访客数（全平台独立用户，默认0）',
    sku_uv BIGINT DEFAULT 0 COMMENT '商品访客数（访问过该商品的独立用户，默认0）',
    sku_pv BIGINT DEFAULT 0 COMMENT '商品浏览量（该商品详情页访问次数，默认0）',
    sku_micro_uv BIGINT DEFAULT 0 COMMENT '商品微详情访客数（访问过该商品微详情的独立用户，默认0）',
    avg_stay_time DECIMAL(10,2) COMMENT '平均停留时长（秒）',
    sku_jump_rate DECIMAL(10,4) COMMENT '商品详情页跳出率',
    sku_click_cnt BIGINT DEFAULT 0 COMMENT '商品点击次数（用户点击该商品的总次数，默认0）',
    dt STRING NOT NULL COMMENT '数据日期（对应原Hive分区字段）'
) COMMENT '商品流量指标日报表'
PARTITIONED BY (ds STRING);



INSERT OVERWRITE TABLE ads_sku_traffic_day PARTITION (ds = '2025-09-22')
SELECT
    -- 商品基础信息
    CAST(sku.sku_id AS STRING) as sku_id,
    sku.sku_name,
    -- 店铺信息（模拟）
    'SHOP_' || CAST(ROW_NUMBER() OVER(PARTITION BY sku.sku_id ORDER BY sku.sku_id) % 10 + 1 AS STRING) as shop_id,
    '店铺_' || CAST(ROW_NUMBER() OVER(PARTITION BY sku.sku_id ORDER BY sku.sku_id) % 10 + 1 AS STRING) as shop_name,
    CAST(sku.tm_id AS STRING) as tm_id,
    sku.tm_name,
    CAST(sku.category3_id AS STRING) as category3_id,
    sku.category3_name,
    -- 总访客数（全平台）
    (SELECT COUNT(DISTINCT user_id) FROM dwd_fact_behavior_detail WHERE ds = '20251019') as total_uv,
    -- 商品访客数
    COUNT(DISTINCT bd.user_id) AS sku_uv,
    -- 商品浏览量（所有行为）
    COUNT(*) AS sku_pv,
    -- 微详情访客数（收藏和加购用户）
    COUNT(DISTINCT CASE WHEN bd.behavior_type IN ('favor', 'cart') THEN bd.user_id END) AS sku_micro_uv,
    -- 平均停留时长（基于行为时间差异估算）
    CAST(
        CASE
            WHEN COUNT(DISTINCT bd.user_id) > 0
            THEN ROUND(COUNT(*) * 30.0 / COUNT(DISTINCT bd.user_id), 2)
            ELSE 0.00
        END AS DECIMAL(10,2)
    ) AS avg_stay_time,
    -- 跳出率（只有一次行为的用户比例）
    CAST(
        ROUND(
            COUNT(DISTINCT CASE WHEN bd_cnt.behavior_count = 1 THEN bd.user_id END) * 1.0 /
            NULLIF(COUNT(DISTINCT bd.user_id), 0),
            4
        ) AS DECIMAL(10,4)
    ) AS sku_jump_rate,
    -- 点击次数（加购数量）
    SUM(CASE WHEN bd.behavior_type = 'cart' THEN bd.cart_num ELSE 0 END) AS sku_click_cnt,
    '2025-09-22' AS dt
FROM dwd_dim_sku_full sku
LEFT JOIN dwd_fact_behavior_detail bd ON sku.sku_id = bd.sku_id AND bd.ds = '20251019'
LEFT JOIN (
    -- 计算每个用户的行为次数
    SELECT user_id, sku_id, COUNT(*) as behavior_count
    FROM dwd_fact_behavior_detail
    WHERE ds = '20251019'
    GROUP BY user_id, sku_id
) bd_cnt ON bd.user_id = bd_cnt.user_id AND bd.sku_id = bd_cnt.sku_id
WHERE sku.ds = '20251019'
GROUP BY
    sku.sku_id, sku.sku_name, sku.tm_id, sku.tm_name,
    sku.category3_id, sku.category3_name;
INSERT OVERWRITE TABLE ads_sku_traffic_day PARTITION (ds = '20251020')
SELECT
    sku_id,
    sku_name,
    shop_id,
    shop_name,
    tm_id,
    tm_name,
    category3_id,
    category3_name,
    total_uv,
    sku_uv,
    sku_pv,
    sku_micro_uv,
    avg_stay_time,
    sku_jump_rate,
    sku_click_cnt,
    dt
FROM (
    -- 小米12S系列 (1-15)
    SELECT '1001' as sku_id, '小米12S Ultra 12GB+512GB 冷杉绿' as sku_name, 'shop_001' as shop_id, '小米官方旗舰店' as shop_name, 'tm_001' as tm_id, '小米' as tm_name, 'cat_001' as category3_id, '智能手机' as category3_name, 18500 as total_uv, 2850 as sku_uv, 6520 as sku_pv, 1850 as sku_micro_uv, 125.50 as avg_stay_time, 0.1850 as sku_jump_rate, 4520 as sku_click_cnt, '2025-10-20' as dt
    UNION ALL SELECT '1002', '小米12S Ultra 12GB+256GB 经典黑', 'shop_001', '小米官方旗舰店', 'tm_001', '小米', 'cat_001', '智能手机', 18500, 2450, 5820, 1620, 118.75, 0.1920, 3980, '2025-10-20'
    UNION ALL SELECT '1003', '小米12S Ultra 8GB+256GB 冷杉绿', 'shop_001', '小米官方旗舰店', 'tm_001', '小米', 'cat_001', '智能手机', 18500, 1980, 4850, 1420, 112.30, 0.2010, 3250, '2025-10-20'
    UNION ALL SELECT '1004', '小米12S Pro 12GB+256GB 黑色', 'shop_001', '小米官方旗舰店', 'tm_001', '小米', 'cat_001', '智能手机', 18500, 2250, 5120, 1680, 108.45, 0.1950, 3850, '2025-10-20'
    UNION ALL SELECT '1005', '小米12S Pro 8GB+256GB 银色', 'shop_001', '小米官方旗舰店', 'tm_001', '小米', 'cat_001', '智能手机', 18500, 1850, 4250, 1350, 105.80, 0.2080, 3120, '2025-10-20'
    UNION ALL SELECT '1006', '小米12S Pro 8GB+128GB 蓝色', 'shop_001', '小米官方旗舰店', 'tm_001', '小米', 'cat_001', '智能手机', 18500, 1520, 3650, 1120, 98.65, 0.2250, 2650, '2025-10-20'
    UNION ALL SELECT '1007', '小米12S 12GB+256GB 白色', 'shop_001', '小米官方旗舰店', 'tm_001', '小米', 'cat_001', '智能手机', 18500, 1980, 4450, 1480, 102.35, 0.1980, 3420, '2025-10-20'
    UNION ALL SELECT '1008', '小米12S 8GB+256GB 黑色', 'shop_001', '小米官方旗舰店', 'tm_001', '小米', 'cat_001', '智能手机', 18500, 1680, 3850, 1250, 96.80, 0.2150, 2980, '2025-10-20'
    UNION ALL SELECT '1009', '小米12S 8GB+128GB 蓝色', 'shop_001', '小米官方旗舰店', 'tm_001', '小米', 'cat_001', '智能手机', 18500, 1420, 3280, 1080, 92.45, 0.2320, 2450, '2025-10-20'
    UNION ALL SELECT '1010', '小米12S Ultra 12GB+512GB 冷杉绿', 'shop_002', '小米授权专卖店', 'tm_001', '小米', 'cat_001', '智能手机', 12500, 1850, 4250, 1420, 118.25, 0.1920, 3250, '2025-10-20'
    UNION ALL SELECT '1011', '小米12S Ultra 12GB+256GB 经典黑', 'shop_002', '小米授权专卖店', 'tm_001', '小米', 'cat_001', '智能手机', 12500, 1520, 3650, 1250, 112.80, 0.2050, 2850, '2025-10-20'
    UNION ALL SELECT '1012', '小米12S Ultra 8GB+256GB 冷杉绿', 'shop_002', '小米授权专卖店', 'tm_001', '小米', 'cat_001', '智能手机', 12500, 1280, 3120, 1080, 108.45, 0.2180, 2450, '2025-10-20'
    UNION ALL SELECT '1013', '小米12S Pro 12GB+256GB 黑色', 'shop_002', '小米授权专卖店', 'tm_001', '小米', 'cat_001', '智能手机', 12500, 1420, 3250, 1180, 105.60, 0.2020, 2680, '2025-10-20'
    UNION ALL SELECT '1014', '小米12S Pro 8GB+256GB 银色', 'shop_002', '小米授权专卖店', 'tm_001', '小米', 'cat_001', '智能手机', 12500, 1180, 2850, 980, 102.35, 0.2150, 2280, '2025-10-20'
    UNION ALL SELECT '1015', '小米12S Pro 8GB+128GB 蓝色', 'shop_002', '小米授权专卖店', 'tm_001', '小米', 'cat_001', '智能手机', 12500, 980, 2450, 850, 95.80, 0.2350, 1980, '2025-10-20'

    -- Redmi系列 (16-30)
    UNION ALL SELECT '2001', 'Redmi 10X 6GB+128GB 冰雾白', 'shop_003', '红米授权专卖店', 'tm_002', 'Redmi', 'cat_001', '智能手机', 22500, 3850, 8520, 2850, 85.25, 0.1520, 6520, '2025-10-20'
    UNION ALL SELECT '2002', 'Redmi 10X 6GB+256GB 冰雾白', 'shop_003', '红米授权专卖店', 'tm_002', 'Redmi', 'cat_001', '智能手机', 22500, 3250, 7250, 2450, 82.60, 0.1650, 5520, '2025-10-20'
    UNION ALL SELECT '2003', 'Redmi Note 12 6GB+128GB 绿色', 'shop_003', '红米授权专卖店', 'tm_002', 'Redmi', 'cat_001', '智能手机', 22500, 4520, 9850, 3250, 78.45, 0.1420, 7850, '2025-10-20'
    UNION ALL SELECT '2004', 'Redmi Note 12 Pro 8GB+128GB 蓝色', 'shop_003', '红米授权专卖店', 'tm_002', 'Redmi', 'cat_001', '智能手机', 22500, 3850, 8450, 2850, 81.80, 0.1550, 6520, '2025-10-20'
    UNION ALL SELECT '2005', 'Redmi 10X 6GB+128GB 冰雾白', 'shop_004', '红米社区店', 'tm_002', 'Redmi', 'cat_001', '智能手机', 18500, 2850, 6520, 2250, 82.35, 0.1580, 4850, '2025-10-20'
    UNION ALL SELECT '2006', 'Redmi 10X 6GB+256GB 冰雾白', 'shop_004', '红米社区店', 'tm_002', 'Redmi', 'cat_001', '智能手机', 18500, 2450, 5820, 1980, 78.90, 0.1720, 4250, '2025-10-20'
    UNION ALL SELECT '2007', 'Redmi Note 12 6GB+128GB 绿色', 'shop_004', '红米社区店', 'tm_002', 'Redmi', 'cat_001', '智能手机', 18500, 3250, 7250, 2450, 75.65, 0.1480, 5520, '2025-10-20'
    UNION ALL SELECT '2008', 'Redmi Note 12 Pro 8GB+128GB 蓝色', 'shop_004', '红米社区店', 'tm_002', 'Redmi', 'cat_001', '智能手机', 18500, 2850, 6520, 2250, 78.20, 0.1620, 4850, '2025-10-20'
    UNION ALL SELECT '2009', 'Redmi 10X 6GB+128GB 冰雾白', 'shop_005', '红米体验店', 'tm_002', 'Redmi', 'cat_001', '智能手机', 16500, 2250, 5120, 1850, 80.45, 0.1650, 3850, '2025-10-20'
    UNION ALL SELECT '2010', 'Redmi 10X 6GB+256GB 冰雾白', 'shop_005', '红米体验店', 'tm_002', 'Redmi', 'cat_001', '智能手机', 16500, 1980, 4520, 1680, 76.80, 0.1780, 3420, '2025-10-20'
    UNION ALL SELECT '2011', 'Redmi Note 12 6GB+128GB 绿色', 'shop_005', '红米体验店', 'tm_002', 'Redmi', 'cat_001', '智能手机', 16500, 2850, 6250, 2150, 72.35, 0.1550, 4850, '2025-10-20'
    UNION ALL SELECT '2012', 'Redmi Note 12 Pro 8GB+128GB 蓝色', 'shop_005', '红米体验店', 'tm_002', 'Redmi', 'cat_001', '智能手机', 16500, 2450, 5520, 1980, 75.60, 0.1680, 4250, '2025-10-20'
    UNION ALL SELECT '2013', 'Redmi K60 8GB+128GB 黑色', 'shop_003', '红米授权专卖店', 'tm_002', 'Redmi', 'cat_001', '智能手机', 22500, 3520, 7850, 2650, 88.75, 0.1620, 6250, '2025-10-20'
    UNION ALL SELECT '2014', 'Redmi K60 12GB+256GB 白色', 'shop_003', '红米授权专卖店', 'tm_002', 'Redmi', 'cat_001', '智能手机', 22500, 2980, 6850, 2350, 85.40, 0.1750, 5520, '2025-10-20'
    UNION ALL SELECT '2015', 'Redmi K60 Pro 12GB+256GB 蓝色', 'shop_003', '红米授权专卖店', 'tm_002', 'Redmi', 'cat_001', '智能手机', 22500, 2650, 6120, 2150, 91.25, 0.1580, 4850, '2025-10-20'

    -- 其他产品系列 (31-50)
    UNION ALL SELECT '3001', '小米平板6 Pro 8GB+256GB 黑色', 'shop_001', '小米官方旗舰店', 'tm_001', '小米', 'cat_002', '平板电脑', 18500, 1850, 4250, 1520, 145.50, 0.1680, 3250, '2025-10-20'
    UNION ALL SELECT '3002', '小米平板6 Pro 12GB+512GB 银色', 'shop_001', '小米官方旗舰店', 'tm_001', '小米', 'cat_002', '平板电脑', 18500, 1520, 3650, 1280, 138.75, 0.1820, 2850, '2025-10-20'
    UNION ALL SELECT '3003', '小米平板6 8GB+128GB 蓝色', 'shop_001', '小米官方旗舰店', 'tm_001', '小米', 'cat_002', '平板电脑', 18500, 1280, 2980, 1080, 132.30, 0.1950, 2250, '2025-10-20'
    UNION ALL SELECT '3004', '小米手表S3 黑色', 'shop_001', '小米官方旗舰店', 'tm_001', '小米', 'cat_003', '智能手表', 18500, 2250, 4850, 1680, 95.25, 0.1420, 3850, '2025-10-20'
    UNION ALL SELECT '3005', '小米手表S3 银色', 'shop_001', '小米官方旗舰店', 'tm_001', '小米', 'cat_003', '智能手表', 18500, 1980, 4250, 1520, 92.80, 0.1550, 3420, '2025-10-20'
    UNION ALL SELECT '3006', '小米蓝牙耳机Air 3 Pro 白色', 'shop_001', '小米官方旗舰店', 'tm_001', '小米', 'cat_004', '耳机', 18500, 2850, 6250, 1980, 68.45, 0.1250, 4850, '2025-10-20'
    UNION ALL SELECT '3007', '小米蓝牙耳机Air 3 Pro 黑色', 'shop_001', '小米官方旗舰店', 'tm_001', '小米', 'cat_004', '耳机', 18500, 2450, 5520, 1850, 65.80, 0.1380, 4250, '2025-10-20'
    UNION ALL SELECT '3008', '小米手环8 Pro 黑色', 'shop_001', '小米官方旗舰店', 'tm_001', '小米', 'cat_003', '智能手表', 18500, 3520, 7520, 2450, 78.35, 0.1180, 5850, '2025-10-20'
    UNION ALL SELECT '3009', '小米手环8 Pro 白色', 'shop_001', '小米官方旗舰店', 'tm_001', '小米', 'cat_003', '智能手表', 18500, 3250, 6850, 2250, 75.60, 0.1320, 5520, '2025-10-20'
    UNION ALL SELECT '3010', '小米充电宝10000mAh 白色', 'shop_001', '小米官方旗舰店', 'tm_001', '小米', 'cat_005', '充电配件', 18500, 4520, 9850, 3250, 45.25, 0.0850, 7850, '2025-10-20'
    UNION ALL SELECT '3011', '小米充电宝20000mAh 黑色', 'shop_001', '小米官方旗舰店', 'tm_001', '小米', 'cat_005', '充电配件', 18500, 3850, 8450, 2850, 42.80, 0.0950, 6850, '2025-10-20'
    UNION ALL SELECT '3012', '小米路由器AX6000', 'shop_001', '小米官方旗舰店', 'tm_001', '小米', 'cat_006', '网络设备', 18500, 1980, 4520, 1680, 112.45, 0.1750, 3420, '2025-10-20'
    UNION ALL SELECT '3013', '小米路由器AX9000', 'shop_001', '小米官方旗舰店', 'tm_001', '小米', 'cat_006', '网络设备', 18500, 1520, 3650, 1420, 125.80, 0.1880, 2850, '2025-10-20'
    UNION ALL SELECT '3014', '小米电视ES65 2023款', 'shop_001', '小米官方旗舰店', 'tm_001', '小米', 'cat_007', '电视', 18500, 1280, 2980, 1180, 185.25, 0.1950, 2250, '2025-10-20'
    UNION ALL SELECT '3015', '小米电视S75 2023款', 'shop_001', '小米官方旗舰店', 'tm_001', '小米', 'cat_007', '电视', 18500, 1080, 2650, 980, 168.75, 0.2080, 1980, '2025-10-20'
    UNION ALL SELECT '3016', '小米平板6 Pro 8GB+256GB 黑色', 'shop_006', '小米平板专卖店', 'tm_001', '小米', 'cat_002', '平板电脑', 12500, 1250, 2850, 980, 142.30, 0.1750, 2250, '2025-10-20'
    UNION ALL SELECT '3017', '小米手表S3 黑色', 'shop_007', '小米穿戴专卖店', 'tm_001', '小米', 'cat_003', '智能手表', 9800, 1520, 3250, 1120, 92.45, 0.1520, 2650, '2025-10-20'
    UNION ALL SELECT '3018', '小米蓝牙耳机Air 3 Pro 白色', 'shop_008', '小米音频专卖店', 'tm_001', '小米', 'cat_004', '耳机', 11500, 1980, 4250, 1420, 65.80, 0.1320, 3420, '2025-10-20'
    UNION ALL SELECT '3019', '小米手环8 Pro 黑色', 'shop_007', '小米穿戴专卖店', 'tm_001', '小米', 'cat_003', '智能手表', 9800, 2250, 4850, 1680, 75.25, 0.1250, 3850, '2025-10-20'
    UNION ALL SELECT '3020', '小米充电宝10000mAh 白色', 'shop_009', '小米配件店', 'tm_001', '小米', 'cat_005', '充电配件', 8500, 2850, 6250, 1980, 42.35, 0.0920, 4850, '2025-10-20'
) t;



-- 2 用户互动指标表
DROP TABLE IF EXISTS ads_sku_conversion_sales_day;
-- 先删除已存在的表
-- 创建表（使用ODPS兼容语法）
CREATE TABLE ads_sku_conversion_sales_day (
    sku_id STRING NOT NULL COMMENT '商品SKU ID',
    sku_name STRING COMMENT '商品名称',
    shop_id STRING COMMENT '店铺ID',
    shop_name STRING COMMENT '店铺名称',
    category3_name STRING COMMENT '三级分类名称',
    pay_conversion_rate DECIMAL(10,4) COMMENT '支付转化率（支付买家数/商品访客数）',
    pay_amt DECIMAL(16,2) COMMENT '支付金额（SKU营收贡献核心指标）',
    pay_num BIGINT DEFAULT 0 COMMENT '支付件数（SKU销量规模，默认0）',
    pay_buyer_num BIGINT DEFAULT 0 COMMENT '支付买家数（避免单用户多买导致的判断偏差，默认0）',
    cart_num BIGINT DEFAULT 0 COMMENT '加购件数（预测潜在成交，辅助库存备货，默认0）',
    cart_buyer_num BIGINT DEFAULT 0 COMMENT '商品加购人数（有加购意愿的用户规模，默认0）',
    favor_buyer_num BIGINT DEFAULT 0 COMMENT '商品收藏人数（反映用户种草意愿，默认0）',
    dt STRING NOT NULL COMMENT '数据日期（对应原Hive分区字段）'
) COMMENT '商品转化效率&SKU销售&用户互动指标日报表'
PARTITIONED BY (ds STRING);


INSERT OVERWRITE TABLE ads_sku_conversion_sales_day PARTITION (ds = '2025-09-22')
SELECT
    -- 商品基础信息
    CAST(sku.sku_id AS STRING) as sku_id,
    sku.sku_name,
    -- 店铺信息（模拟）
    'SHOP_' || CAST(ROW_NUMBER() OVER(PARTITION BY sku.sku_id ORDER BY sku.sku_id) % 10 + 1 AS STRING) as shop_id,
    '店铺_' || CAST(ROW_NUMBER() OVER(PARTITION BY sku.sku_id ORDER BY sku.sku_id) % 10 + 1 AS STRING) as shop_name,
    sku.category3_name,
    -- 支付转化率（支付买家数/商品访客数）
    CAST(
        ROUND(
            COUNT(DISTINCT td.user_id) * 1.0 /
            NULLIF(COUNT(DISTINCT bd.user_id), 0),
        4) AS DECIMAL(10,4)
    ) as pay_conversion_rate,
    -- 支付金额
    CAST(COALESCE(SUM(td.total_amount), 0) AS DECIMAL(16,2)) as pay_amt,
    -- 支付件数
    COALESCE(SUM(td.sku_num), 0) as pay_num,
    -- 支付买家数
    COUNT(DISTINCT td.user_id) as pay_buyer_num,
    -- 加购件数（所有加购行为）
    SUM(CASE WHEN bd.behavior_type = 'cart' THEN bd.cart_num ELSE 0 END) as cart_num,
    -- 加购人数
    COUNT(DISTINCT CASE WHEN bd.behavior_type = 'cart' THEN bd.user_id END) as cart_buyer_num,
    -- 收藏人数
    COUNT(DISTINCT CASE WHEN bd.behavior_type = 'favor' THEN bd.user_id END) as favor_buyer_num,
    '2025-09-22' as dt
FROM dwd_dim_sku_full sku
LEFT JOIN dwd_fact_behavior_detail bd ON sku.sku_id = bd.sku_id AND bd.ds = '20251019'
LEFT JOIN dwd_fact_trade_detail td ON sku.sku_id = td.sku_id AND td.ds = '20251019'
WHERE sku.ds = '20251019'
GROUP BY
    sku.sku_id, sku.sku_name, sku.category3_name;

-- 插入50条商品转化销售数据（ds=20251020）
INSERT OVERWRITE TABLE ads_sku_conversion_sales_day PARTITION (ds = '20251020')
SELECT
    CAST(sku_id AS STRING) as sku_id,
    sku_name,
    CAST(shop_id AS STRING) as shop_id,
    shop_name,
    category3_name,
    CAST(pay_conversion_rate AS DECIMAL(10,4)) as pay_conversion_rate,
    CAST(pay_amt AS DECIMAL(16,2)) as pay_amt,
    pay_num,
    pay_buyer_num,
    cart_num,
    cart_buyer_num,
    favor_buyer_num,
    '2025-10-20' as dt
FROM (
    -- 小米12S Ultra系列 (1-9)
    SELECT 1 as sku_id, '小米12S Ultra 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 12GB+512GB 冷杉绿 5G手机' as sku_name, 17 as shop_id, '小米全品类旗舰店' as shop_name, '智能手机' as category3_name, 0.1850 as pay_conversion_rate, 285600.50 as pay_amt, 128 as pay_num, 85 as pay_buyer_num, 256 as cart_num, 156 as cart_buyer_num, 142 as favor_buyer_num
    UNION ALL SELECT 1, '小米12S Ultra 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 12GB+512GB 冷杉绿 5G手机', 2, '小米12S系列专卖店', '智能手机', 0.1920, 198400.00, 96, 62, 168, 102, 88
    UNION ALL SELECT 1, '小米12S Ultra 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 12GB+512GB 冷杉绿 5G手机', 1, '小米旗舰手机体验店', '智能手机', 0.1780, 324800.25, 152, 98, 310, 189, 165

    SELECT 2, '小米12S Ultra 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 12GB+256GB 经典黑 5G手机', 17, '小米全品类旗舰店', '智能手机', 0.1880, 245200.75, 118, 76, 232, 141, 128
    UNION ALL SELECT 2, '小米12S Ultra 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 12GB+256GB 经典黑 5G手机', 2, '小米12S系列专卖店', '智能手机', 0.1950, 172800.50, 84, 54, 142, 86, 78
    UNION ALL SELECT 2, '小米12S Ultra 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 12GB+256GB 经典黑 5G手机', 1, '小米旗舰手机体验店', '智能手机', 0.1820, 298400.00, 144, 93, 285, 174, 152

    SELECT 3, '小米12S Ultra 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 8GB+256GB 冷杉绿 5G手机', 17, '小米全品类旗舰店', '智能手机', 0.1760, 201600.25, 112, 72, 198, 121, 108
    UNION ALL SELECT 3, '小米12S Ultra 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 8GB+256GB 冷杉绿 5G手机', 2, '小米12S系列专卖店', '智能手机', 0.1840, 145600.75, 88, 57, 125, 76, 68
    UNION ALL SELECT 3, '小米12S Ultra 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 8GB+256GB 冷杉绿 5G手机', 1, '小米旗舰手机体验店', '智能手机', 0.1710, 264800.50, 136, 88, 245, 149, 132

    -- 小米12S Pro系列 (10-18)
    SELECT 4, '小米12S Pro 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 12GB+256GB 黑色 5G手机', 17, '小米全品类旗舰店', '智能手机', 0.1920, 218400.00, 112, 72, 210, 128, 118
    UNION ALL SELECT 4, '小米12S Pro 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 12GB+256GB 黑色 5G手机', 2, '小米12S系列专卖店', '智能手机', 0.1980, 156800.25, 96, 62, 152, 93, 85
    UNION ALL SELECT 4, '小米12S Pro 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 12GB+256GB 黑色 5G手机', 1, '小米旗舰手机体验店', '智能手机', 0.1850, 289600.75, 148, 95, 268, 163, 148

    SELECT 5, '小米12S Pro 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 8GB+256GB 银色 5G手机', 17, '小米全品类旗舰店', '智能手机', 0.1810, 183200.50, 104, 67, 185, 113, 102
    UNION ALL SELECT 5, '小米12S Pro 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 8GB+256GB 银色 5G手机', 2, '小米12S系列专卖店', '智能手机', 0.1890, 132800.00, 80, 52, 138, 84, 76
    UNION ALL SELECT 5, '小米12S Pro 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 8GB+256GB 银色 5G手机', 1, '小米旗舰手机体验店', '智能手机', 0.1760, 245600.25, 128, 82, 232, 141, 128

    SELECT 6, '小米12S Pro 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 8GB+128GB 蓝色 5G手机', 17, '小米全品类旗舰店', '智能手机', 0.1740, 156800.75, 96, 62, 168, 102, 92
    UNION ALL SELECT 6, '小米12S Pro 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 8GB+128GB 蓝色 5G手机', 2, '小米12S系列专卖店', '智能手机', 0.1820, 112400.50, 72, 46, 125, 76, 68
    UNION ALL SELECT 6, '小米12S Pro 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 8GB+128GB 蓝色 5G手机', 1, '小米旗舰手机体验店', '智能手机', 0.1690, 201600.00, 112, 72, 198, 121, 108

    -- 小米12S系列 (19-27)
    SELECT 7, '小米12S 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 12GB+256GB 白色 5G手机', 17, '小米全品类旗舰店', '智能手机', 0.1870, 189600.25, 108, 69, 198, 121, 112
    UNION ALL SELECT 7, '小米12S 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 12GB+256GB 白色 5G手机', 2, '小米12S系列专卖店', '智能手机', 0.1940, 136800.50, 84, 54, 152, 93, 85
    UNION ALL SELECT 7, '小米12S 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 12GB+256GB 白色 5G手机', 1, '小米旗舰手机体验店', '智能手机', 0.1800, 256800.75, 136, 88, 245, 149, 138

    SELECT 8, '小米12S 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 8GB+256GB 黑色 5G手机', 17, '小米全品类旗舰店', '智能手机', 0.1790, 164000.00, 100, 64, 175, 107, 98
    UNION ALL SELECT 8, '小米12S 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 8GB+256GB 黑色 5G手机', 2, '小米12S系列专卖店', '智能手机', 0.1860, 118400.25, 76, 49, 132, 80, 72
    UNION ALL SELECT 8, '小米12S 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 8GB+256GB 黑色 5G手机', 1, '小米旗舰手机体验店', '智能手机', 0.1730, 224800.50, 124, 80, 218, 133, 122

    SELECT 9, '小米12S 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 8GB+128GB 蓝色 5G手机', 17, '小米全品类旗舰店', '智能手机', 0.1720, 140800.75, 88, 57, 152, 93, 85
    UNION ALL SELECT 9, '小米12S 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 8GB+128GB 蓝色 5G手机', 2, '小米12S系列专卖店', '智能手机', 0.1800, 101600.00, 68, 44, 118, 72, 65
    UNION ALL SELECT 9, '小米12S 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 8GB+128GB 蓝色 5G手机', 1, '小米旗舰手机体验店', '智能手机', 0.1670, 189600.25, 108, 69, 185, 113, 102

    -- Redmi系列 (28-41)
    SELECT 10, 'Redmi 10X 4G Helio G85游戏芯 4800万超清四摄 5020mAh大电量 小孔全面屏 128GB大存储 6GB+128GB 冰雾白 游戏智能手机', 4, '红米10X授权门店', '智能手机', 0.2050, 82400.50, 152, 98, 285, 174, 158
    UNION ALL SELECT 10, 'Redmi 10X 4G Helio G85游戏芯 4800万超清四摄 5020mAh大电量 小孔全面屏 128GB大存储 6GB+128GB 冰雾白 游戏智能手机', 5, '红米授权体验店', '智能手机', 0.2120, 68800.75, 128, 82, 232, 141, 128
    UNION ALL SELECT 10, 'Redmi 10X 4G Helio G85游戏芯 4800万超清四摄 5020mAh大电量 小孔全面屏 128GB大存储 6GB+128GB 冰雾白 游戏智能手机', 6, '红米社区店', '智能手机', 0.1980, 75600.00, 140, 90, 268, 163, 148

    SELECT 11, 'Redmi 10X 4G Helio G85游戏芯 4800万超清四摄 5020mAh大电量 小孔全面屏 256GB大存储 6GB+256GB 冰雾白 游戏智能手机', 4, '红米10X授权门店', '智能手机', 0.1920, 92800.25, 136, 88, 245, 149, 138
    UNION ALL SELECT 11, 'Redmi 10X 4G Helio G85游戏芯 4800万超清四摄 5020mAh大电量 小孔全面屏 256GB大存储 6GB+256GB 冰雾白 游戏智能手机', 5, '红米授权体验店', '智能手机', 0.1990, 77600.50, 116, 75, 198, 121, 112
    UNION ALL SELECT 11, 'Redmi 10X 4G Helio G85游戏芯 4800万超清四摄 5020mAh大电量 小孔全面屏 256GB大存储 6GB+256GB 冰雾白 游戏智能手机', 6, '红米社区店', '智能手机', 0.1850, 85200.75, 124, 80, 218, 133, 122

    SELECT 12, 'Redmi Note 12 骁龙4 Gen1 5000mAh大电量 1080P全高清屏 128GB存储 6GB+128GB 绿色 智能手机', 4, '红米10X授权门店', '智能手机', 0.2180, 65400.00, 168, 108, 325, 198, 182
    UNION ALL SELECT 12, 'Redmi Note 12 骁龙4 Gen1 5000mAh大电量 1080P全高清屏 128GB存储 6GB+128GB 绿色 智能手机', 5, '红米授权体验店', '智能手机', 0.2250, 54800.25, 144, 93, 268, 163, 152
    UNION ALL SELECT 12, 'Redmi Note 12 骁龙4 Gen1 5000mAh大电量 1080P全高清屏 128GB存储 6GB+128GB 绿色 智能手机', 6, '红米社区店', '智能手机', 0.2110, 60200.50, 156, 100, 298, 182, 168

    SELECT 13, 'Redmi Note 12 Pro 天玑1080 120Hz OLED直屏 5000mAh大电量 8GB+128GB 蓝色 智能手机', 4, '红米10X授权门店', '智能手机', 0.2010, 89200.75, 148, 95, 275, 168, 155
    UNION ALL SELECT 13, 'Redmi Note 12 Pro 天玑1080 120Hz OLED直屏 5000mAh大电量 8GB+128GB 蓝色 智能手机', 5, '红米授权体验店', '智能手机', 0.2080, 74800.00, 132, 85, 232, 141, 132
    UNION ALL SELECT 13, 'Redmi Note 12 Pro 天玑1080 120Hz OLED直屏 5000mAh大电量 8GB+128GB 蓝色 智能手机', 6, '红米社区店', '智能手机', 0.1940, 82400.25, 140, 90, 258, 157, 145

    -- 其他产品系列 (42-50)
    SELECT 14, '小米平板6 Pro 11英寸 2.8K超清屏 骁龙8+ 144Hz高刷 8600mAh 67W快充 8GB+256GB 黑色', 17, '小米全品类旗舰店', '平板电脑', 0.1680, 189600.50, 96, 62, 185, 113, 105
    UNION ALL SELECT 14, '小米平板6 Pro 11英寸 2.8K超清屏 骁龙8+ 144Hz高刷 8600mAh 67W快充 8GB+256GB 黑色', 3, '小米平板专卖店', '平板电脑', 0.1750, 142200.75, 76, 49, 142, 86, 82
    UNION ALL SELECT 14, '小米平板6 Pro 11英寸 2.8K超清屏 骁龙8+ 144Hz高刷 8600mAh 67W快充 8GB+256GB 黑色', 1, '小米旗舰手机体验店', '平板电脑', 0.1620, 236400.00, 116, 75, 225, 137, 128

    SELECT 15, '小米手表S3 1.43英寸AMOLED屏 独立通话 血氧心率监测 12天续航 蓝牙版 黑色', 17, '小米全品类旗舰店', '智能手表', 0.1950, 87600.25, 168, 108, 285, 174, 165
    UNION ALL SELECT 15, '小米手表S3 1.43英寸AMOLED屏 独立通话 血氧心率监测 12天续航 蓝牙版 黑色', 7, '小米穿戴设备店', '智能手表', 0.2020, 65400.50, 132, 85, 218, 133, 128
    UNION ALL SELECT 15, '小米手表S3 1.43英寸AMOLED屏 独立通话 血氧心率监测 12天续航 蓝牙版 黑色', 1, '小米旗舰手机体验店', '智能手表', 0.1880, 109800.75, 196, 126, 352, 215, 202

    SELECT 16, '小米蓝牙耳机Air 3 Pro 主动降噪 无线充电 超长续航 28小时 白色', 17, '小米全品类旗舰店', '耳机', 0.2250, 67800.00, 224, 144, 385, 235, 225
    UNION ALL SELECT 16, '小米蓝牙耳机Air 3 Pro 主动降噪 无线充电 超长续航 28小时 白色', 8, '小米音频专卖店', '耳机', 0.2320, 51200.25, 180, 116, 298, 182, 175
    UNION ALL SELECT 16, '小米蓝牙耳机Air 3 Pro 主动降噪 无线充电 超长续航 28小时 白色', 1, '小米旗舰手机体验店', '耳机', 0.2180, 84400.50, 268, 172, 472, 288, 275
) t;


INSERT OVERWRITE TABLE ads_sku_conversion_sales_day PARTITION (ds = '20251020')
SELECT
    CAST(sku_id AS STRING) as sku_id,
    sku_name,
    CAST(shop_id AS STRING) as shop_id,
    shop_name,
    category3_name,
    CAST(pay_conversion_rate AS DECIMAL(10,4)) as pay_conversion_rate,
    CAST(pay_amt AS DECIMAL(16,2)) as pay_amt,
    pay_num,
    pay_buyer_num,
    cart_num,
    cart_buyer_num,
    favor_buyer_num,
    '2025-10-20' as dt
FROM (
    -- 小米12S Ultra系列 (1-9)
    SELECT 1 as sku_id, '小米12S Ultra 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 12GB+512GB 冷杉绿 5G手机' as sku_name, 17 as shop_id, '小米全品类旗舰店' as shop_name, '智能手机' as category3_name, 0.1850 as pay_conversion_rate, 285600.50 as pay_amt, 128 as pay_num, 85 as pay_buyer_num, 256 as cart_num, 156 as cart_buyer_num, 142 as favor_buyer_num
    UNION ALL SELECT 1, '小米12S Ultra 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 12GB+512GB 冷杉绿 5G手机', 2, '小米12S系列专卖店', '智能手机', 0.1920, 198400.00, 96, 62, 168, 102, 88
    UNION ALL SELECT 1, '小米12S Ultra 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 12GB+512GB 冷杉绿 5G手机', 1, '小米旗舰手机体验店', '智能手机', 0.1780, 324800.25, 152, 98, 310, 189, 165

    UNION ALL SELECT 2, '小米12S Ultra 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 12GB+256GB 经典黑 5G手机', 17, '小米全品类旗舰店', '智能手机', 0.1880, 245200.75, 118, 76, 232, 141, 128
    UNION ALL SELECT 2, '小米12S Ultra 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 12GB+256GB 经典黑 5G手机', 2, '小米12S系列专卖店', '智能手机', 0.1950, 172800.50, 84, 54, 142, 86, 78
    UNION ALL SELECT 2, '小米12S Ultra 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 12GB+256GB 经典黑 5G手机', 1, '小米旗舰手机体验店', '智能手机', 0.1820, 298400.00, 144, 93, 285, 174, 152

    UNION ALL SELECT 3, '小米12S Ultra 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 8GB+256GB 冷杉绿 5G手机', 17, '小米全品类旗舰店', '智能手机', 0.1760, 201600.25, 112, 72, 198, 121, 108
    UNION ALL SELECT 3, '小米12S Ultra 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 8GB+256GB 冷杉绿 5G手机', 2, '小米12S系列专卖店', '智能手机', 0.1840, 145600.75, 88, 57, 125, 76, 68
    UNION ALL SELECT 3, '小米12S Ultra 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 8GB+256GB 冷杉绿 5G手机', 1, '小米旗舰手机体验店', '智能手机', 0.1710, 264800.50, 136, 88, 245, 149, 132

    -- 小米12S Pro系列 (10-18)
    UNION ALL SELECT 4, '小米12S Pro 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 12GB+256GB 黑色 5G手机', 17, '小米全品类旗舰店', '智能手机', 0.1920, 218400.00, 112, 72, 210, 128, 118
    UNION ALL SELECT 4, '小米12S Pro 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 12GB+256GB 黑色 5G手机', 2, '小米12S系列专卖店', '智能手机', 0.1980, 156800.25, 96, 62, 152, 93, 85
    UNION ALL SELECT 4, '小米12S Pro 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 12GB+256GB 黑色 5G手机', 1, '小米旗舰手机体验店', '智能手机', 0.1850, 289600.75, 148, 95, 268, 163, 148

    UNION ALL SELECT 5, '小米12S Pro 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 8GB+256GB 银色 5G手机', 17, '小米全品类旗舰店', '智能手机', 0.1810, 183200.50, 104, 67, 185, 113, 102
    UNION ALL SELECT 5, '小米12S Pro 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 8GB+256GB 银色 5G手机', 2, '小米12S系列专卖店', '智能手机', 0.1890, 132800.00, 80, 52, 138, 84, 76
    UNION ALL SELECT 5, '小米12S Pro 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 8GB+256GB 银色 5G手机', 1, '小米旗舰手机体验店', '智能手机', 0.1760, 245600.25, 128, 82, 232, 141, 128

    UNION ALL SELECT 6, '小米12S Pro 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 8GB+128GB 蓝色 5G手机', 17, '小米全品类旗舰店', '智能手机', 0.1740, 156800.75, 96, 62, 168, 102, 92
    UNION ALL SELECT 6, '小米12S Pro 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 8GB+128GB 蓝色 5G手机', 2, '小米12S系列专卖店', '智能手机', 0.1820, 112400.50, 72, 46, 125, 76, 68
    UNION ALL SELECT 6, '小米12S Pro 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 8GB+128GB 蓝色 5G手机', 1, '小米旗舰手机体验店', '智能手机', 0.1690, 201600.00, 112, 72, 198, 121, 108

    -- 小米12S系列 (19-27)
    UNION ALL SELECT 7, '小米12S 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 12GB+256GB 白色 5G手机', 17, '小米全品类旗舰店', '智能手机', 0.1870, 189600.25, 108, 69, 198, 121, 112
    UNION ALL SELECT 7, '小米12S 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 12GB+256GB 白色 5G手机', 2, '小米12S系列专卖店', '智能手机', 0.1940, 136800.50, 84, 54, 152, 93, 85
    UNION ALL SELECT 7, '小米12S 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 12GB+256GB 白色 5G手机', 1, '小米旗舰手机体验店', '智能手机', 0.1800, 256800.75, 136, 88, 245, 149, 138

    UNION ALL SELECT 8, '小米12S 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 8GB+256GB 黑色 5G手机', 17, '小米全品类旗舰店', '智能手机', 0.1790, 164000.00, 100, 64, 175, 107, 98
    UNION ALL SELECT 8, '小米12S 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 8GB+256GB 黑色 5G手机', 2, '小米12S系列专卖店', '智能手机', 0.1860, 118400.25, 76, 49, 132, 80, 72
    UNION ALL SELECT 8, '小米12S 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 8GB+256GB 黑色 5G手机', 1, '小米旗舰手机体验店', '智能手机', 0.1730, 224800.50, 124, 80, 218, 133, 122

    UNION ALL SELECT 9, '小米12S 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 8GB+128GB 蓝色 5G手机', 17, '小米全品类旗舰店', '智能手机', 0.1720, 140800.75, 88, 57, 152, 93, 85
    UNION ALL SELECT 9, '小米12S 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 8GB+128GB 蓝色 5G手机', 2, '小米12S系列专卖店', '智能手机', 0.1800, 101600.00, 68, 44, 118, 72, 65
    UNION ALL SELECT 9, '小米12S 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 8GB+128GB 蓝色 5G手机', 1, '小米旗舰手机体验店', '智能手机', 0.1670, 189600.25, 108, 69, 185, 113, 102

    -- Redmi系列 (28-41)
    UNION ALL SELECT 10, 'Redmi 10X 4G Helio G85游戏芯 4800万超清四摄 5020mAh大电量 小孔全面屏 128GB大存储 6GB+128GB 冰雾白 游戏智能手机', 4, '红米10X授权门店', '智能手机', 0.2050, 82400.50, 152, 98, 285, 174, 158
    UNION ALL SELECT 10, 'Redmi 10X 4G Helio G85游戏芯 4800万超清四摄 5020mAh大电量 小孔全面屏 128GB大存储 6GB+128GB 冰雾白 游戏智能手机', 5, '红米授权体验店', '智能手机', 0.2120, 68800.75, 128, 82, 232, 141, 128
    UNION ALL SELECT 10, 'Redmi 10X 4G Helio G85游戏芯 4800万超清四摄 5020mAh大电量 小孔全面屏 128GB大存储 6GB+128GB 冰雾白 游戏智能手机', 6, '红米社区店', '智能手机', 0.1980, 75600.00, 140, 90, 268, 163, 148

    UNION ALL SELECT 11, 'Redmi 10X 4G Helio G85游戏芯 4800万超清四摄 5020mAh大电量 小孔全面屏 256GB大存储 6GB+256GB 冰雾白 游戏智能手机', 4, '红米10X授权门店', '智能手机', 0.1920, 92800.25, 136, 88, 245, 149, 138
    UNION ALL SELECT 11, 'Redmi 10X 4G Helio G85游戏芯 4800万超清四摄 5020mAh大电量 小孔全面屏 256GB大存储 6GB+256GB 冰雾白 游戏智能手机', 5, '红米授权体验店', '智能手机', 0.1990, 77600.50, 116, 75, 198, 121, 112
    UNION ALL SELECT 11, 'Redmi 10X 4G Helio G85游戏芯 4800万超清四摄 5020mAh大电量 小孔全面屏 256GB大存储 6GB+256GB 冰雾白 游戏智能手机', 6, '红米社区店', '智能手机', 0.1850, 85200.75, 124, 80, 218, 133, 122

    UNION ALL SELECT 12, 'Redmi Note 12 骁龙4 Gen1 5000mAh大电量 1080P全高清屏 128GB存储 6GB+128GB 绿色 智能手机', 4, '红米10X授权门店', '智能手机', 0.2180, 65400.00, 168, 108, 325, 198, 182
    UNION ALL SELECT 12, 'Redmi Note 12 骁龙4 Gen1 5000mAh大电量 1080P全高清屏 128GB存储 6GB+128GB 绿色 智能手机', 5, '红米授权体验店', '智能手机', 0.2250, 54800.25, 144, 93, 268, 163, 152
    UNION ALL SELECT 12, 'Redmi Note 12 骁龙4 Gen1 5000mAh大电量 1080P全高清屏 128GB存储 6GB+128GB 绿色 智能手机', 6, '红米社区店', '智能手机', 0.2110, 60200.50, 156, 100, 298, 182, 168

    UNION ALL SELECT 13, 'Redmi Note 12 Pro 天玑1080 120Hz OLED直屏 5000mAh大电量 8GB+128GB 蓝色 智能手机', 4, '红米10X授权门店', '智能手机', 0.2010, 89200.75, 148, 95, 275, 168, 155
    UNION ALL SELECT 13, 'Redmi Note 12 Pro 天玑1080 120Hz OLED直屏 5000mAh大电量 8GB+128GB 蓝色 智能手机', 5, '红米授权体验店', '智能手机', 0.2080, 74800.00, 132, 85, 232, 141, 132
    UNION ALL SELECT 13, 'Redmi Note 12 Pro 天玑1080 120Hz OLED直屏 5000mAh大电量 8GB+128GB 蓝色 智能手机', 6, '红米社区店', '智能手机', 0.1940, 82400.25, 140, 90, 258, 157, 145

    -- 其他产品系列 (42-50)
    UNION ALL SELECT 14, '小米平板6 Pro 11英寸 2.8K超清屏 骁龙8+ 144Hz高刷 8600mAh 67W快充 8GB+256GB 黑色', 17, '小米全品类旗舰店', '平板电脑', 0.1680, 189600.50, 96, 62, 185, 113, 105
    UNION ALL SELECT 14, '小米平板6 Pro 11英寸 2.8K超清屏 骁龙8+ 144Hz高刷 8600mAh 67W快充 8GB+256GB 黑色', 3, '小米平板专卖店', '平板电脑', 0.1750, 142200.75, 76, 49, 142, 86, 82
    UNION ALL SELECT 14, '小米平板6 Pro 11英寸 2.8K超清屏 骁龙8+ 144Hz高刷 8600mAh 67W快充 8GB+256GB 黑色', 1, '小米旗舰手机体验店', '平板电脑', 0.1620, 236400.00, 116, 75, 225, 137, 128

    UNION ALL SELECT 15, '小米手表S3 1.43英寸AMOLED屏 独立通话 血氧心率监测 12天续航 蓝牙版 黑色', 17, '小米全品类旗舰店', '智能手表', 0.1950, 87600.25, 168, 108, 285, 174, 165
    UNION ALL SELECT 15, '小米手表S3 1.43英寸AMOLED屏 独立通话 血氧心率监测 12天续航 蓝牙版 黑色', 7, '小米穿戴设备店', '智能手表', 0.2020, 65400.50, 132, 85, 218, 133, 128
    UNION ALL SELECT 15, '小米手表S3 1.43英寸AMOLED屏 独立通话 血氧心率监测 12天续航 蓝牙版 黑色', 1, '小米旗舰手机体验店', '智能手表', 0.1880, 109800.75, 196, 126, 352, 215, 202

    UNION ALL SELECT 16, '小米蓝牙耳机Air 3 Pro 主动降噪 无线充电 超长续航 28小时 白色', 17, '小米全品类旗舰店', '耳机', 0.2250, 67800.00, 224, 144, 385, 235, 225
    UNION ALL SELECT 16, '小米蓝牙耳机Air 3 Pro 主动降噪 无线充电 超长续航 28小时 白色', 8, '小米音频专卖店', '耳机', 0.2320, 51200.25, 180, 116, 298, 182, 175
    UNION ALL SELECT 16, '小米蓝牙耳机Air 3 Pro 主动降噪 无线充电 超长续航 28小时 白色', 1, '小米旗舰手机体验店', '耳机', 0.2180, 84400.50, 268, 172, 472, 288, 275
) t;


-- 3 商品店铺收藏&加购表
DROP TABLE IF EXISTS ads_sku_user_interaction_day;
-- 创建表（ODPS兼容语法）
CREATE TABLE IF NOT EXISTS ads_sku_user_interaction_day (
    sku_id STRING NOT NULL COMMENT '商品SKU ID',
    sku_name STRING COMMENT '商品名称（适配长商品名，如含规格描述的名称）',
    shop_id STRING COMMENT '店铺ID',
    shop_name STRING COMMENT '店铺名称',
    tm_name STRING COMMENT '品牌名称',
    favor_buyer_num BIGINT DEFAULT 0 COMMENT '商品收藏人数（反映种草意愿，默认0）',
    cart_buyer_num BIGINT DEFAULT 0 COMMENT '商品加购人数（加购意愿用户规模，默认0）',
    dt STRING NOT NULL COMMENT '数据日期（对应原Hive分区字段）'
) COMMENT '商品用户互动&粉丝指标日报表'
PARTITIONED BY (ds STRING);

-- 插入数据
INSERT OVERWRITE TABLE ads_sku_user_interaction_day PARTITION (ds = '2025-09-22')
SELECT
    CAST(sku.sku_id AS STRING) as sku_id,
    sku.sku_name,
    -- 店铺信息（模拟）
    'SHOP_' || CAST(ROW_NUMBER() OVER(PARTITION BY sku.sku_id ORDER BY sku.sku_id) % 10 + 1 AS STRING) as shop_id,
    '店铺_' || CAST(ROW_NUMBER() OVER(PARTITION BY sku.sku_id ORDER BY sku.sku_id) % 10 + 1 AS STRING) as shop_name,
    sku.tm_name,
    -- 收藏人数
    COUNT(DISTINCT CASE WHEN bd.behavior_type = 'favor' THEN bd.user_id END) as favor_buyer_num,
    -- 加购人数
    COUNT(DISTINCT CASE WHEN bd.behavior_type = 'cart' THEN bd.user_id END) as cart_buyer_num,
    '2025-09-22' as dt
FROM dwd_dim_sku_full sku
LEFT JOIN dwd_fact_behavior_detail bd ON sku.sku_id = bd.sku_id AND bd.ds = '20251019'
WHERE sku.ds = '20251019'
GROUP BY
    sku.sku_id, sku.sku_name, sku.tm_name;

-- 商品用户互动数据批量插入（2025-10-15 至 2025-10-21）
-- 使用INSERT OVERWRITE并指定分区
INSERT OVERWRITE TABLE ads_sku_user_interaction_day PARTITION (ds)
SELECT
    CAST(sku_id AS STRING) as sku_id,
    sku_name,
    CAST(shop_id AS STRING) as shop_id,
    shop_name,
    tm_name,
    favor_buyer_num,
    cart_buyer_num,
    dt,
    -- 添加分区字段，使用dt作为分区值
    dt as ds
FROM (
    -- 2025-10-15 数据（小米12S Ultra系列）
    SELECT 1 as sku_id, '小米12S Ultra 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 12GB+512GB 冷杉绿 5G手机' as sku_name, 1 as shop_id, '小米旗舰手机体验店' as shop_name, '小米' as tm_name, 186 as favor_buyer_num, 215 as cart_buyer_num, '2025-10-15' as dt
    UNION ALL SELECT 1, '小米12S Ultra 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 12GB+512GB 冷杉绿 5G手机', 17, '小米全品类旗舰店', '小米', 228, 256, '2025-10-15'
    UNION ALL SELECT 2, '小米12S Ultra 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 12GB+256GB 经典黑 5G手机', 2, '小米12S系列专卖店', '小米', 142, 168, '2025-10-15'
    -- 2025-10-15 数据（苹果iPhone 12系列）
    UNION ALL SELECT 10, 'Apple iPhone 12 (A2404) 64GB 蓝色 支持移动联通电信5G 双卡双待手机', 5, 'Apple官方授权店', '苹果', 412, 455, '2025-10-15'
    UNION ALL SELECT 11, 'Apple iPhone 12 (A2404) 64GB 白色 支持移动联通电信5G 双卡双待手机', 6, 'iPhone 12体验专区', '苹果', 352, 388, '2025-10-15'
    UNION ALL SELECT 12, 'Apple iPhone 12 (A2404) 128GB 黑色 支持移动联通电信5G 双卡双待手机', 18, '苹果小米综合数码店', '苹果', 346, 385, '2025-10-15'

    -- 2025-10-16 数据（小米12S Ultra系列，收藏+3%~5%）
    UNION ALL SELECT 1, '小米12S Ultra 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 12GB+512GB 冷杉绿 5G手机', 1, '小米旗舰手机体验店', '小米', 192, 222, '2025-10-16'
    UNION ALL SELECT 1, '小米12S Ultra 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 12GB+512GB 冷杉绿 5G手机', 17, '小米全品类旗舰店', '小米', 236, 265, '2025-10-16'
    UNION ALL SELECT 2, '小米12S Ultra 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 12GB+256GB 经典黑 5G手机', 2, '小米12S系列专卖店', '小米', 148, 175, '2025-10-16'
    -- 2025-10-16 数据（苹果iPhone 12系列，收藏+2%~4%）
    UNION ALL SELECT 10, 'Apple iPhone 12 (A2404) 64GB 蓝色 支持移动联通电信5G 双卡双待手机', 5, 'Apple官方授权店', '苹果', 420, 463, '2025-10-16'
    UNION ALL SELECT 11, 'Apple iPhone 12 (A2404) 64GB 白色 支持移动联通电信5G 双卡双待手机', 6, 'iPhone 12体验专区', '苹果', 360, 396, '2025-10-16'
    UNION ALL SELECT 12, 'Apple iPhone 12 (A2404) 128GB 黑色 支持移动联通电信5G 双卡双待手机', 18, '苹果小米综合数码店', '苹果', 354, 393, '2025-10-16'

    -- 2025-10-17 数据（小米12S Pro系列新增）
    UNION ALL SELECT 3, '小米12S Pro 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 8GB+256GB 银色 5G手机', 1, '小米旗舰手机体验店', '小米', 135, 158, '2025-10-17'
    UNION ALL SELECT 3, '小米12S Pro 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 8GB+256GB 银色 5G手机', 17, '小米全品类旗舰店', '小米', 172, 195, '2025-10-17'
    -- 2025-10-17 数据（苹果iPhone 12 mini系列新增）
    UNION ALL SELECT 7, 'Apple iPhone 12 mini (A2400) 64GB 粉色 支持移动联通电信5G 双卡双待手机', 5, 'Apple官方授权店', '苹果', 328, 362, '2025-10-17'
    UNION ALL SELECT 7, 'Apple iPhone 12 mini (A2400) 64GB 粉色 支持移动联通电信5G 双卡双待手机', 6, 'iPhone 12体验专区', '苹果', 302, 335, '2025-10-17'
    -- 2025-10-17 数据（原有机型小幅波动）
    UNION ALL SELECT 1, '小米12S Ultra 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 12GB+512GB 冷杉绿 5G手机', 1, '小米旗舰手机体验店', '小米', 195, 226, '2025-10-17'
    UNION ALL SELECT 10, 'Apple iPhone 12 (A2404) 64GB 蓝色 支持移动联通电信5G 双卡双待手机', 5, 'Apple官方授权店', '苹果', 425, 468, '2025-10-17'

    -- 2025-10-18 数据（小米12S系列新增）
    UNION ALL SELECT 6, '小米12S 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 8GB+128GB 蓝色 5G手机', 2, '小米12S系列专卖店', '小米', 98, 120, '2025-10-18'
    UNION ALL SELECT 6, '小米12S 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 8GB+128GB 蓝色 5G手机', 17, '小米全品类旗舰店', '小米', 148, 172, '2025-10-18'
    -- 2025-10-18 数据（苹果iPhone 12 256GB新增）
    UNION ALL SELECT 5, 'Apple iPhone 12 (A2404) 256GB 黑色 支持移动联通电信5G 双卡双待手机', 5, 'Apple官方授权店', '苹果', 352, 385, '2025-10-18'
    UNION ALL SELECT 5, 'Apple iPhone 12 (A2404) 256GB 黑色 支持移动联通电信5G 双卡双待手机', 18, '苹果小米综合数码店', '苹果', 285, 318, '2025-10-18'
    -- 2025-10-18 数据（原有机型波动）
    UNION ALL SELECT 2, '小米12S Ultra 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 12GB+256GB 经典黑 5G手机', 2, '小米12S系列专卖店', '小米', 150, 177, '2025-10-18'
    UNION ALL SELECT 11, 'Apple iPhone 12 (A2404) 64GB 白色 支持移动联通电信5G 双卡双待手机', 6, 'iPhone 12体验专区', '苹果', 365, 402, '2025-10-18'

    -- 2025-10-19 数据（小米12S Ultra 8GB版新增）
    UNION ALL SELECT 8, '小米12S Ultra 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 8GB+256GB 冷杉绿 5G手机', 1, '小米旗舰手机体验店', '小米', 142, 165, '2025-10-19'
    UNION ALL SELECT 8, '小米12S Ultra 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 8GB+256GB 冷杉绿 5G手机', 17, '小米全品类旗舰店', '小米', 180, 205, '2025-10-19'
    -- 2025-10-19 数据（苹果iPhone 12 mini 128GB新增）
    UNION ALL SELECT 14, 'Apple iPhone 12 mini (A2400) 128GB 蓝色 支持移动联通电信5G 双卡双待手机', 5, 'Apple官方授权店', '苹果', 305, 338, '2025-10-19'
    UNION ALL SELECT 14, 'Apple iPhone 12 mini (A2400) 128GB 蓝色 支持移动联通电信5G 双卡双待手机', 6, 'iPhone 12体验专区', '苹果', 282, 312, '2025-10-19'
    -- 2025-10-19 数据（原有机型波动）
    UNION ALL SELECT 3, '小米12S Pro 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 8GB+256GB 银色 5G手机', 1, '小米旗舰手机体验店', '小米', 138, 162, '2025-10-19'
    UNION ALL SELECT 12, 'Apple iPhone 12 (A2404) 128GB 黑色 支持移动联通电信5G 双卡双待手机', 18, '苹果小米综合数码店', '苹果', 358, 398, '2025-10-19'

    -- 2025-10-20 数据（小米12S Pro 12GB版新增）
    UNION ALL SELECT 13, '小米12S Pro 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 12GB+256GB 黑色 5G手机', 1, '小米旗舰手机体验店', '小米', 155, 180, '2025-10-20'
    UNION ALL SELECT 13, '小米12S Pro 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 12GB+256GB 黑色 5G手机', 17, '小米全品类旗舰店', '小米', 198, 225, '2025-10-20'
    -- 2025-10-20 数据（苹果iPhone 12 64GB黑色新增）
    UNION ALL SELECT 16, 'Apple iPhone 12 (A2404) 64GB 黑色 支持移动联通电信5G 双卡双待手机', 5, 'Apple官方授权店', '苹果', 385, 420, '2025-10-20'
    UNION ALL SELECT 16, 'Apple iPhone 12 (A2404) 64GB 黑色 支持移动联通电信5G 双卡双待手机', 6, 'iPhone 12体验专区', '苹果', 358, 392, '2025-10-20'
    -- 2025-10-20 数据（原有机型波动）
    UNION ALL SELECT 7, 'Apple iPhone 12 mini (A2400) 64GB 粉色 支持移动联通电信5G 双卡双待手机', 5, 'Apple官方授权店', '苹果', 332, 366, '2025-10-20'
    UNION ALL SELECT 6, '小米12S 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 8GB+128GB 蓝色 5G手机', 17, '小米全品类旗舰店', '小米', 152, 176, '2025-10-20'

    -- 2025-10-21 数据（小米12S Ultra 128GB版新增）
    UNION ALL SELECT 17, '小米12S Ultra 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 12GB+128GB 经典黑 5G手机', 1, '小米旗舰手机体验店', '小米', 135, 158, '2025-10-21'
    UNION ALL SELECT 17, '小米12S Ultra 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 12GB+128GB 经典黑 5G手机', 17, '小米全品类旗舰店', '小米', 172, 195, '2025-10-21'
    -- 2025-10-21 数据（苹果iPhone 12 256GB白色新增）
    UNION ALL SELECT 9, 'Apple iPhone 12 (A2404) 256GB 白色 支持移动联通电信5G 双卡双待手机', 5, 'Apple官方授权店', '苹果', 342, 375, '2025-10-21'
    UNION ALL SELECT 9, 'Apple iPhone 12 (A2404) 256GB 白色 支持移动联通电信5G 双卡双待手机', 18, '苹果小米综合数码店', '苹果', 275, 308, '2025-10-21'
    -- 2025-10-21 数据（原有机型收尾）
    UNION ALL SELECT 1, '小米12S Ultra 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 12GB+512GB 冷杉绿 5G手机', 17, '小米全品类旗舰店', '小米', 245, 278, '2025-10-21'
    UNION ALL SELECT 10, 'Apple iPhone 12 (A2404) 64GB 蓝色 支持移动联通电信5G 双卡双待手机', 5, 'Apple官方授权店', '苹果', 438, 482, '2025-10-21'
    UNION ALL SELECT 2, '小米12S Ultra 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 12GB+256GB 经典黑 5G手机', 2, '小米12S系列专卖店', '小米', 155, 182, '2025-10-21'
    UNION ALL SELECT 11, 'Apple iPhone 12 (A2404) 64GB 白色 支持移动联通电信5G 双卡双待手机', 6, 'iPhone 12体验专区', '苹果', 372, 408, '2025-10-21'
) t;

INSERT OVERWRITE TABLE ads_sku_user_interaction_day PARTITION (ds = '20251020')
SELECT
    sku_id,
    sku_name,
    shop_id,
    shop_name,
    tm_name,
    favor_buyer_num,
    cart_buyer_num,
    dt
FROM (
    -- 小米12S系列 (1-15)
    SELECT '1001' as sku_id, '小米12S Ultra 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 12GB+512GB 冷杉绿 5G手机' as sku_name, 'shop_001' as shop_id, '小米官方旗舰店' as shop_name, '小米' as tm_name, 285 as favor_buyer_num, 452 as cart_buyer_num, '2025-10-20' as dt
    UNION ALL SELECT '1002', '小米12S Ultra 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 12GB+256GB 经典黑 5G手机', 'shop_001', '小米官方旗舰店', '小米', 245, 398, '2025-10-20'
    UNION ALL SELECT '1003', '小米12S Ultra 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 8GB+256GB 冷杉绿 5G手机', 'shop_001', '小米官方旗舰店', '小米', 198, 342, '2025-10-20'
    UNION ALL SELECT '1004', '小米12S Pro 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 12GB+256GB 黑色 5G手机', 'shop_001', '小米官方旗舰店', '小米', 225, 385, '2025-10-20'
    UNION ALL SELECT '1005', '小米12S Pro 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 8GB+256GB 银色 5G手机', 'shop_001', '小米官方旗舰店', '小米', 185, 325, '2025-10-20'
    UNION ALL SELECT '1006', '小米12S Pro 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 8GB+128GB 蓝色 5G手机', 'shop_001', '小米官方旗舰店', '小米', 152, 285, '2025-10-20'
    UNION ALL SELECT '1007', '小米12S 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 12GB+256GB 白色 5G手机', 'shop_001', '小米官方旗舰店', '小米', 198, 352, '2025-10-20'
    UNION ALL SELECT '1008', '小米12S 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 8GB+256GB 黑色 5G手机', 'shop_001', '小米官方旗舰店', '小米', 168, 312, '2025-10-20'
    UNION ALL SELECT '1009', '小米12S 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 8GB+128GB 蓝色 5G手机', 'shop_001', '小米官方旗舰店', '小米', 142, 268, '2025-10-20'
    UNION ALL SELECT '1010', '小米12S Ultra 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 12GB+512GB 冷杉绿 5G手机', 'shop_002', '小米授权专卖店', '小米', 185, 325, '2025-10-20'
    UNION ALL SELECT '1011', '小米12S Ultra 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 12GB+256GB 经典黑 5G手机', 'shop_002', '小米授权专卖店', '小米', 152, 285, '2025-10-20'
    UNION ALL SELECT '1012', '小米12S Ultra 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 8GB+256GB 冷杉绿 5G手机', 'shop_002', '小米授权专卖店', '小米', 128, 245, '2025-10-20'
    UNION ALL SELECT '1013', '小米12S Pro 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 12GB+256GB 黑色 5G手机', 'shop_002', '小米授权专卖店', '小米', 142, 268, '2025-10-20'
    UNION ALL SELECT '1014', '小米12S Pro 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 8GB+256GB 银色 5G手机', 'shop_002', '小米授权专卖店', '小米', 118, 228, '2025-10-20'
    UNION ALL SELECT '1015', '小米12S Pro 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 8GB+128GB 蓝色 5G手机', 'shop_002', '小米授权专卖店', '小米', 98, 198, '2025-10-20'

    -- Redmi系列 (16-30)
    UNION ALL SELECT '2001', 'Redmi 10X 4G Helio G85游戏芯 4800万超清四摄 5020mAh大电量 小孔全面屏 128GB大存储 6GB+128GB 冰雾白 游戏智能手机', 'shop_003', '红米授权专卖店', 'Redmi', 385, 625, '2025-10-20'
    UNION ALL SELECT '2002', 'Redmi 10X 4G Helio G85游戏芯 4800万超清四摄 5020mAh大电量 小孔全面屏 256GB大存储 6GB+256GB 冰雾白 游戏智能手机', 'shop_003', '红米授权专卖店', 'Redmi', 325, 552, '2025-10-20'
    UNION ALL SELECT '2003', 'Redmi Note 12 骁龙4 Gen1 5000mAh大电量 1080P全高清屏 128GB存储 6GB+128GB 绿色 智能手机', 'shop_003', '红米授权专卖店', 'Redmi', 452, 725, '2025-10-20'
    UNION ALL SELECT '2004', 'Redmi Note 12 Pro 天玑1080 120Hz OLED直屏 5000mAh大电量 8GB+128GB 蓝色 智能手机', 'shop_003', '红米授权专卖店', 'Redmi', 385, 652, '2025-10-20'
    UNION ALL SELECT '2005', 'Redmi 10X 4G Helio G85游戏芯 4800万超清四摄 5020mAh大电量 小孔全面屏 128GB大存储 6GB+128GB 冰雾白 游戏智能手机', 'shop_004', '红米社区店', 'Redmi', 285, 485, '2025-10-20'
    UNION ALL SELECT '2006', 'Redmi 10X 4G Helio G85游戏芯 4800万超清四摄 5020mAh大电量 小孔全面屏 256GB大存储 6GB+256GB 冰雾白 游戏智能手机', 'shop_004', '红米社区店', 'Redmi', 245, 425, '2025-10-20'
    UNION ALL SELECT '2007', 'Redmi Note 12 骁龙4 Gen1 5000mAh大电量 1080P全高清屏 128GB存储 6GB+128GB 绿色 智能手机', 'shop_004', '红米社区店', 'Redmi', 325, 552, '2025-10-20'
    UNION ALL SELECT '2008', 'Redmi Note 12 Pro 天玑1080 120Hz OLED直屏 5000mAh大电量 8GB+128GB 蓝色 智能手机', 'shop_004', '红米社区店', 'Redmi', 285, 485, '2025-10-20'
    UNION ALL SELECT '2009', 'Redmi 10X 4G Helio G85游戏芯 4800万超清四摄 5020mAh大电量 小孔全面屏 128GB大存储 6GB+128GB 冰雾白 游戏智能手机', 'shop_005', '红米体验店', 'Redmi', 225, 385, '2025-10-20'
    UNION ALL SELECT '2010', 'Redmi 10X 4G Helio G85游戏芯 4800万超清四摄 5020mAh大电量 小孔全面屏 256GB大存储 6GB+256GB 冰雾白 游戏智能手机', 'shop_005', '红米体验店', 'Redmi', 198, 342, '2025-10-20'
    UNION ALL SELECT '2011', 'Redmi Note 12 骁龙4 Gen1 5000mAh大电量 1080P全高清屏 128GB存储 6GB+128GB 绿色 智能手机', 'shop_005', '红米体验店', 'Redmi', 285, 485, '2025-10-20'
    UNION ALL SELECT '2012', 'Redmi Note 12 Pro 天玑1080 120Hz OLED直屏 5000mAh大电量 8GB+128GB 蓝色 智能手机', 'shop_005', '红米体验店', 'Redmi', 245, 425, '2025-10-20'
    UNION ALL SELECT '2013', 'Redmi K60 骁龙8+ 2K高光屏 6400万超清相机 5500mAh长续航 67W快充 8GB+128GB 黑色 5G手机', 'shop_003', '红米授权专卖店', 'Redmi', 352, 585, '2025-10-20'
    UNION ALL SELECT '2014', 'Redmi K60 骁龙8+ 2K高光屏 6400万超清相机 5500mAh长续航 67W快充 12GB+256GB 白色 5G手机', 'shop_003', '红米授权专卖店', 'Redmi', 298, 512, '2025-10-20'
    UNION ALL SELECT '2015', 'Redmi K60 Pro 骁龙8 Gen2 2K高光屏 IMX800主摄 5000mAh 120W快充 12GB+256GB 蓝色 5G手机', 'shop_003', '红米授权专卖店', 'Redmi', 265, 452, '2025-10-20'

    -- 其他产品系列 (31-50)
    UNION ALL SELECT '3001', '小米平板6 Pro 11英寸 2.8K超清屏 骁龙8+ 144Hz高刷 8600mAh 67W快充 8GB+256GB 黑色', 'shop_001', '小米官方旗舰店', '小米', 185, 325, '2025-10-20'
    UNION ALL SELECT '3002', '小米平板6 Pro 11英寸 2.8K超清屏 骁龙8+ 144Hz高刷 8600mAh 67W快充 12GB+512GB 银色', 'shop_001', '小米官方旗舰店', '小米', 152, 285, '2025-10-20'
    UNION ALL SELECT '3003', '小米平板6 11英寸 2.8K超清屏 骁龙870 144Hz高刷 8840mAh 33W快充 8GB+128GB 蓝色', 'shop_001', '小米官方旗舰店', '小米', 128, 245, '2025-10-20'
    UNION ALL SELECT '3004', '小米手表S3 1.43英寸AMOLED屏 独立通话 血氧心率监测 12天续航 蓝牙版 黑色', 'shop_001', '小米官方旗舰店', '小米', 225, 385, '2025-10-20'
    UNION ALL SELECT '3005', '小米手表S3 1.43英寸AMOLED屏 独立通话 血氧心率监测 12天续航 蓝牙版 银色', 'shop_001', '小米官方旗舰店', '小米', 198, 352, '2025-10-20'
    UNION ALL SELECT '3006', '小米蓝牙耳机Air 3 Pro 主动降噪 无线充电 超长续航 28小时 白色', 'shop_001', '小米官方旗舰店', '小米', 285, 485, '2025-10-20'
    UNION ALL SELECT '3007', '小米蓝牙耳机Air 3 Pro 主动降噪 无线充电 超长续航 28小时 黑色', 'shop_001', '小米官方旗舰店', '小米', 245, 425, '2025-10-20'
    UNION ALL SELECT '3008', '小米手环8 Pro 1.74英寸AMOLED大屏 独立GPS 血氧心率监测 14天续航 黑色', 'shop_001', '小米官方旗舰店', '小米', 352, 585, '2025-10-20'
    UNION ALL SELECT '3009', '小米手环8 Pro 1.74英寸AMOLED大屏 独立GPS 血氧心率监测 14天续航 白色', 'shop_001', '小米官方旗舰店', '小米', 325, 552, '2025-10-20'
    UNION ALL SELECT '3010', '小米充电宝10000mAh 22.5W快充 Type-C接口 双向快充 白色', 'shop_001', '小米官方旗舰店', '小米', 452, 725, '2025-10-20'
    UNION ALL SELECT '3011', '小米充电宝20000mAh 50W快充 Type-C接口 双向快充 黑色', 'shop_001', '小米官方旗舰店', '小米', 385, 652, '2025-10-20'
    UNION ALL SELECT '3012', '小米路由器AX6000 WiFi6 6000M速率 8数据流 6核处理器 黑色', 'shop_001', '小米官方旗舰店', '小米', 198, 342, '2025-10-20'
    UNION ALL SELECT '3013', '小米路由器AX9000 WiFi6增强版 9000M速率 三频 高通6核处理器 黑色', 'shop_001', '小米官方旗舰店', '小米', 152, 285, '2025-10-20'
    UNION ALL SELECT '3014', '小米电视ES65 2023款 4K超高清 多分区背光 MEMC运动补偿 3+32GB 金属全面屏', 'shop_001', '小米官方旗舰店', '小米', 128, 245, '2025-10-20'
    UNION ALL SELECT '3015', '小米电视S75 2023款 4K超高清 120Hz高刷 多分区背光 3+32GB 金属全面屏', 'shop_001', '小米官方旗舰店', '小米', 108, 225, '2025-10-20'
    UNION ALL SELECT '3016', '小米平板6 Pro 11英寸 2.8K超清屏 骁龙8+ 144Hz高刷 8600mAh 67W快充 8GB+256GB 黑色', 'shop_006', '小米平板专卖店', '小米', 125, 225, '2025-10-20'
    UNION ALL SELECT '3017', '小米手表S3 1.43英寸AMOLED屏 独立通话 血氧心率监测 12天续航 蓝牙版 黑色', 'shop_007', '小米穿戴专卖店', '小米', 152, 265, '2025-10-20'
    UNION ALL SELECT '3018', '小米蓝牙耳机Air 3 Pro 主动降噪 无线充电 超长续航 28小时 白色', 'shop_008', '小米音频专卖店', '小米', 198, 342, '2025-10-20'
    UNION ALL SELECT '3019', '小米手环8 Pro 1.74英寸AMOLED大屏 独立GPS 血氧心率监测 14天续航 黑色', 'shop_007', '小米穿戴专卖店', '小米', 225, 385, '2025-10-20'
    UNION ALL SELECT '3020', '小米充电宝10000mAh 22.5W快充 Type-C接口 双向快充 白色', 'shop_009', '小米配件店', '小米', 285, 485, '2025-10-20'
) t;

-- 4 商品新老用户表
DROP TABLE IF EXISTS ads_sku_customer_insight_day;
CREATE TABLE ads_sku_customer_insight_day (
    sku_id STRING NOT NULL COMMENT '商品SKU ID',
    sku_name STRING COMMENT '商品名称（适配含规格的长商品名，如"小米14 12GB+256GB 黑色"）',
    shop_id STRING COMMENT '商品所属店铺ID',
    shop_name STRING COMMENT '商品所属店铺名称',
    category3_name STRING COMMENT '三级分类名称',
    total_visitor_num BIGINT COMMENT '商品总访客数（用于占比计算）',
    new_user_num BIGINT COMMENT '新用户数（商品访客中的新用户）',
    old_user_num BIGINT COMMENT '老用户数（商品访客中的老用户）',
    new_old_ratio DECIMAL(10,4) COMMENT '新老用户占比（新用户数/总访客数，对应指标13）',
    male_ratio DECIMAL(10,4) COMMENT '男性用户占比（预测性别占比，对应指标14）',
    female_ratio DECIMAL(10,4) COMMENT '女性用户占比（预测性别占比，对应指标14）',
    dt STRING NOT NULL COMMENT '数据日期（对应原Hive分区字段）'
) COMMENT '商品客群洞察指标日报表（新老用户+性别占比）'
PARTITIONED BY (ds STRING);

-- 插入数据示例
INSERT OVERWRITE TABLE ads_sku_customer_insight_day PARTITION (ds = '20250922')
SELECT
    CAST(sku.sku_id AS STRING) as sku_id,
    sku.sku_name,
    'SHOP_' || CAST(ROW_NUMBER() OVER(PARTITION BY sku.sku_id ORDER BY sku.sku_id) % 10 + 1 AS STRING) as shop_id,
    '店铺_' || CAST(ROW_NUMBER() OVER(PARTITION BY sku.sku_id ORDER BY sku.sku_id) % 10 + 1 AS STRING) as shop_name,
    sku.category3_name,
    COUNT(DISTINCT bd.user_id) as total_visitor_num,
    CAST(COUNT(DISTINCT bd.user_id) * (0.3 + RAND() * 0.2) AS BIGINT) as new_user_num,
    COUNT(DISTINCT bd.user_id) - CAST(COUNT(DISTINCT bd.user_id) * (0.3 + RAND() * 0.2) AS BIGINT) as old_user_num,
    CAST((0.3 + RAND() * 0.2) AS DECIMAL(10,4)) as new_old_ratio,
    CAST((0.5 + RAND() * 0.2) AS DECIMAL(10,4)) as male_ratio,
    CAST((0.3 + RAND() * 0.2) AS DECIMAL(10,4)) as female_ratio,
    '2025-09-22' as dt
FROM dwd_dim_sku_full sku
LEFT JOIN dwd_fact_behavior_detail bd ON sku.sku_id = bd.sku_id AND bd.ds = '20251019'
WHERE sku.ds = '20251019'
GROUP BY
    sku.sku_id, sku.sku_name, sku.category3_name;

INSERT OVERWRITE TABLE ads_sku_customer_insight_day PARTITION (ds = '20251020')
SELECT
    sku_id,
    sku_name,
    shop_id,
    shop_name,
    category3_name,
    total_visitor_num,
    new_user_num,
    old_user_num,
    new_old_ratio,
    male_ratio,
    female_ratio,
    dt
FROM (
    -- 小米12S系列 (1-10)
    SELECT '1001' as sku_id, '小米12S Ultra 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 12GB+512GB 冷杉绿 5G手机' as sku_name, 'shop_001' as shop_id, '小米官方旗舰店' as shop_name, '智能手机' as category3_name, 2850 as total_visitor_num, 855 as new_user_num, 1995 as old_user_num, 0.3000 as new_old_ratio, 0.7520 as male_ratio, 0.2480 as female_ratio, '2025-10-20' as dt
    UNION ALL SELECT '1002', '小米12S Ultra 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 12GB+256GB 经典黑 5G手机', 'shop_001', '小米官方旗舰店', '智能手机', 2450, 735, 1715, 0.3000, 0.7680, 0.2320, '2025-10-20'
    UNION ALL SELECT '1003', '小米12S Ultra 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 8GB+256GB 冷杉绿 5G手机', 'shop_001', '小米官方旗舰店', '智能手机', 1980, 594, 1386, 0.3000, 0.7450, 0.2550, '2025-10-20'
    UNION ALL SELECT '1004', '小米12S Pro 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 12GB+256GB 黑色 5G手机', 'shop_001', '小米官方旗舰店', '智能手机', 2250, 675, 1575, 0.3000, 0.7420, 0.2580, '2025-10-20'
    UNION ALL SELECT '1005', '小米12S Pro 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 8GB+256GB 银色 5G手机', 'shop_001', '小米官方旗舰店', '智能手机', 1850, 555, 1295, 0.3000, 0.7350, 0.2650, '2025-10-20'
    UNION ALL SELECT '1006', '小米12S Pro 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 8GB+128GB 蓝色 5G手机', 'shop_001', '小米官方旗舰店', '智能手机', 1520, 456, 1064, 0.3000, 0.7280, 0.2720, '2025-10-20'
    UNION ALL SELECT '1007', '小米12S 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 12GB+256GB 白色 5G手机', 'shop_001', '小米官方旗舰店', '智能手机', 1980, 594, 1386, 0.3000, 0.7180, 0.2820, '2025-10-20'
    UNION ALL SELECT '1008', '小米12S 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 8GB+256GB 黑色 5G手机', 'shop_001', '小米官方旗舰店', '智能手机', 1680, 504, 1176, 0.3000, 0.7250, 0.2750, '2025-10-20'
    UNION ALL SELECT '1009', '小米12S 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 8GB+128GB 蓝色 5G手机', 'shop_001', '小米官方旗舰店', '智能手机', 1420, 426, 994, 0.3000, 0.7120, 0.2880, '2025-10-20'
    UNION ALL SELECT '1010', '小米12S Ultra 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 12GB+512GB 冷杉绿 5G手机', 'shop_002', '小米授权专卖店', '智能手机', 1850, 555, 1295, 0.3000, 0.7480, 0.2520, '2025-10-20'

    -- Redmi系列 (11-20)
    UNION ALL SELECT '2001', 'Redmi 10X 4G Helio G85游戏芯 4800万超清四摄 5020mAh大电量 小孔全面屏 128GB大存储 6GB+128GB 冰雾白 游戏智能手机', 'shop_003', '红米授权专卖店', '智能手机', 3850, 1540, 2310, 0.4000, 0.6850, 0.3150, '2025-10-20'
    UNION ALL SELECT '2002', 'Redmi 10X 4G Helio G85游戏芯 4800万超清四摄 5020mAh大电量 小孔全面屏 256GB大存储 6GB+256GB 冰雾白 游戏智能手机', 'shop_003', '红米授权专卖店', '智能手机', 3250, 1300, 1950, 0.4000, 0.6780, 0.3220, '2025-10-20'
    UNION ALL SELECT '2003', 'Redmi Note 12 骁龙4 Gen1 5000mAh大电量 1080P全高清屏 128GB存储 6GB+128GB 绿色 智能手机', 'shop_003', '红米授权专卖店', '智能手机', 4520, 1808, 2712, 0.4000, 0.6920, 0.3080, '2025-10-20'
    UNION ALL SELECT '2004', 'Redmi Note 12 Pro 天玑1080 120Hz OLED直屏 5000mAh大电量 8GB+128GB 蓝色 智能手机', 'shop_003', '红米授权专卖店', '智能手机', 3850, 1540, 2310, 0.4000, 0.6850, 0.3150, '2025-10-20'
    UNION ALL SELECT '2005', 'Redmi 10X 4G Helio G85游戏芯 4800万超清四摄 5020mAh大电量 小孔全面屏 128GB大存储 6GB+128GB 冰雾白 游戏智能手机', 'shop_004', '红米社区店', '智能手机', 2850, 1140, 1710, 0.4000, 0.6720, 0.3280, '2025-10-20'
    UNION ALL SELECT '2006', 'Redmi 10X 4G Helio G85游戏芯 4800万超清四摄 5020mAh大电量 小孔全面屏 256GB大存储 6GB+256GB 冰雾白 游戏智能手机', 'shop_004', '红米社区店', '智能手机', 2450, 980, 1470, 0.4000, 0.6650, 0.3350, '2025-10-20'
    UNION ALL SELECT '2007', 'Redmi Note 12 骁龙4 Gen1 5000mAh大电量 1080P全高清屏 128GB存储 6GB+128GB 绿色 智能手机', 'shop_004', '红米社区店', '智能手机', 3250, 1300, 1950, 0.4000, 0.6820, 0.3180, '2025-10-20'
    UNION ALL SELECT '2008', 'Redmi Note 12 Pro 天玑1080 120Hz OLED直屏 5000mAh大电量 8GB+128GB 蓝色 智能手机', 'shop_004', '红米社区店', '智能手机', 2850, 1140, 1710, 0.4000, 0.6750, 0.3250, '2025-10-20'
    UNION ALL SELECT '2009', 'Redmi 10X 4G Helio G85游戏芯 4800万超清四摄 5020mAh大电量 小孔全面屏 128GB大存储 6GB+128GB 冰雾白 游戏智能手机', 'shop_005', '红米体验店', '智能手机', 2250, 900, 1350, 0.4000, 0.6680, 0.3320, '2025-10-20'
    UNION ALL SELECT '2010', 'Redmi 10X 4G Helio G85游戏芯 4800万超清四摄 5020mAh大电量 小孔全面屏 256GB大存储 6GB+256GB 冰雾白 游戏智能手机', 'shop_005', '红米体验店', '智能手机', 1980, 792, 1188, 0.4000, 0.6620, 0.3380, '2025-10-20'

    -- 平板电脑系列 (21-30)
    UNION ALL SELECT '3001', '小米平板6 Pro 11英寸 2.8K超清屏 骁龙8+ 144Hz高刷 8600mAh 67W快充 8GB+256GB 黑色', 'shop_001', '小米官方旗舰店', '平板电脑', 1850, 740, 1110, 0.4000, 0.6250, 0.3750, '2025-10-20'
    UNION ALL SELECT '3002', '小米平板6 Pro 11英寸 2.8K超清屏 骁龙8+ 144Hz高刷 8600mAh 67W快充 12GB+512GB 银色', 'shop_001', '小米官方旗舰店', '平板电脑', 1520, 608, 912, 0.4000, 0.6180, 0.3820, '2025-10-20'
    UNION ALL SELECT '3003', '小米平板6 11英寸 2.8K超清屏 骁龙870 144Hz高刷 8840mAh 33W快充 8GB+128GB 蓝色', 'shop_001', '小米官方旗舰店', '平板电脑', 1280, 512, 768, 0.4000, 0.6120, 0.3880, '2025-10-20'
    UNION ALL SELECT '3004', '小米平板6 Pro 11英寸 2.8K超清屏 骁龙8+ 144Hz高刷 8600mAh 67W快充 8GB+256GB 黑色', 'shop_006', '小米平板专卖店', '平板电脑', 1250, 500, 750, 0.4000, 0.6280, 0.3720, '2025-10-20'
    UNION ALL SELECT '3005', '小米平板6 11英寸 2.8K超清屏 骁龙870 144Hz高刷 8840mAh 33W快充 8GB+128GB 蓝色', 'shop_006', '小米平板专卖店', '平板电脑', 980, 392, 588, 0.4000, 0.6150, 0.3850, '2025-10-20'
    UNION ALL SELECT '3006', 'Redmi Pad SE 11英寸 90Hz 护眼屏 8000mAh 18W快充 8GB+128GB 深空灰', 'shop_003', '红米授权专卖店', '平板电脑', 1680, 672, 1008, 0.4000, 0.6050, 0.3950, '2025-10-20'
    UNION ALL SELECT '3007', 'Redmi Pad SE 11英寸 90Hz 护眼屏 8000mAh 18W快充 6GB+128GB 烟青绿', 'shop_003', '红米授权专卖店', '平板电脑', 1420, 568, 852, 0.4000, 0.5980, 0.4020, '2025-10-20'
    UNION ALL SELECT '3008', '小米平板6 Pro 11英寸 2.8K超清屏 骁龙8+ 144Hz高刷 8600mAh 67W快充 8GB+256GB 黑色', 'shop_002', '小米授权专卖店', '平板电脑', 850, 340, 510, 0.4000, 0.6220, 0.3780, '2025-10-20'
    UNION ALL SELECT '3009', '小米平板6 11英寸 2.8K超清屏 骁龙870 144Hz高刷 8840mAh 33W快充 8GB+128GB 蓝色', 'shop_002', '小米授权专卖店', '平板电脑', 720, 288, 432, 0.4000, 0.6080, 0.3920, '2025-10-20'
    UNION ALL SELECT '3010', 'Redmi Pad SE 11英寸 90Hz 护眼屏 8000mAh 18W快充 8GB+128GB 深空灰', 'shop_004', '红米社区店', '平板电脑', 1250, 500, 750, 0.4000, 0.5950, 0.4050, '2025-10-20'

    -- 智能穿戴设备 (31-40)
    UNION ALL SELECT '4001', '小米手表S3 1.43英寸AMOLED屏 独立通话 血氧心率监测 12天续航 蓝牙版 黑色', 'shop_001', '小米官方旗舰店', '智能手表', 2250, 900, 1350, 0.4000, 0.6820, 0.3180, '2025-10-20'
    UNION ALL SELECT '4002', '小米手表S3 1.43英寸AMOLED屏 独立通话 血氧心率监测 12天续航 蓝牙版 银色', 'shop_001', '小米官方旗舰店', '智能手表', 1980, 792, 1188, 0.4000, 0.6750, 0.3250, '2025-10-20'
    UNION ALL SELECT '4003', '小米手环8 Pro 1.74英寸AMOLED大屏 独立GPS 血氧心率监测 14天续航 黑色', 'shop_001', '小米官方旗舰店', '智能手表', 3520, 1408, 2112, 0.4000, 0.6550, 0.3450, '2025-10-20'
    UNION ALL SELECT '4004', '小米手环8 Pro 1.74英寸AMOLED大屏 独立GPS 血氧心率监测 14天续航 白色', 'shop_001', '小米官方旗舰店', '智能手表', 3250, 1300, 1950, 0.4000, 0.6480, 0.3520, '2025-10-20'
    UNION ALL SELECT '4005', '小米手表S3 1.43英寸AMOLED屏 独立通话 血氧心率监测 12天续航 蓝牙版 黑色', 'shop_007', '小米穿戴专卖店', '智能手表', 1520, 608, 912, 0.4000, 0.6780, 0.3220, '2025-10-20'
    UNION ALL SELECT '4006', '小米手环8 Pro 1.74英寸AMOLED大屏 独立GPS 血氧心率监测 14天续航 黑色', 'shop_007', '小米穿戴专卖店', '智能手表', 2250, 900, 1350, 0.4000, 0.6520, 0.3480, '2025-10-20'
    UNION ALL SELECT '4007', 'Redmi Watch 4 1.97英寸AMOLED屏 独立GPS 血氧监测 10天续航 黑色', 'shop_003', '红米授权专卖店', '智能手表', 1850, 740, 1110, 0.4000, 0.6420, 0.3580, '2025-10-20'
    UNION ALL SELECT '4008', 'Redmi Watch 4 1.97英寸AMOLED屏 独立GPS 血氧监测 10天续航 白色', 'shop_003', '红米授权专卖店', '智能手表', 1680, 672, 1008, 0.4000, 0.6350, 0.3650, '2025-10-20'
    UNION ALL SELECT '4009', '小米手表S3 1.43英寸AMOLED屏 独立通话 血氧心率监测 12天续航 蓝牙版 黑色', 'shop_002', '小米授权专卖店', '智能手表', 980, 392, 588, 0.4000, 0.6720, 0.3280, '2025-10-20'
    UNION ALL SELECT '4010', '小米手环8 Pro 1.74英寸AMOLED大屏 独立GPS 血氧心率监测 14天续航 黑色', 'shop_002', '小米授权专卖店', '智能手表', 1250, 500, 750, 0.4000, 0.6450, 0.3550, '2025-10-20'

    -- 音频设备和配件 (41-50)
    UNION ALL SELECT '5001', '小米蓝牙耳机Air 3 Pro 主动降噪 无线充电 超长续航 28小时 白色', 'shop_001', '小米官方旗舰店', '耳机', 2850, 1140, 1710, 0.4000, 0.6250, 0.3750, '2025-10-20'
    UNION ALL SELECT '5002', '小米蓝牙耳机Air 3 Pro 主动降噪 无线充电 超长续航 28小时 黑色', 'shop_001', '小米官方旗舰店', '耳机', 2450, 980, 1470, 0.4000, 0.6320, 0.3680, '2025-10-20'
    UNION ALL SELECT '5003', 'Redmi Buds 5 Pro 主动降噪 无线充电 40小时续航 白色', 'shop_003', '红米授权专卖店', '耳机', 3250, 1300, 1950, 0.4000, 0.6180, 0.3820, '2025-10-20'
    UNION ALL SELECT '5004', 'Redmi Buds 5 Pro 主动降噪 无线充电 40小时续航 黑色', 'shop_003', '红米授权专卖店', '耳机', 2850, 1140, 1710, 0.4000, 0.6250, 0.3750, '2025-10-20'
    UNION ALL SELECT '5005', '小米充电宝10000mAh 22.5W快充 Type-C接口 双向快充 白色', 'shop_001', '小米官方旗舰店', '充电配件', 4520, 1808, 2712, 0.4000, 0.5850, 0.4150, '2025-10-20'
    UNION ALL SELECT '5006', '小米充电宝20000mAh 50W快充 Type-C接口 双向快充 黑色', 'shop_001', '小米官方旗舰店', '充电配件', 3850, 1540, 2310, 0.4000, 0.5920, 0.4080, '2025-10-20'
    UNION ALL SELECT '5007', '小米充电宝10000mAh 22.5W快充 Type-C接口 双向快充 白色', 'shop_009', '小米配件店', '充电配件', 2850, 1140, 1710, 0.4000, 0.5780, 0.4220, '2025-10-20'
    UNION ALL SELECT '5008', '小米路由器AX6000 WiFi6 6000M速率 8数据流 6核处理器 黑色', 'shop_001', '小米官方旗舰店', '网络设备', 1980, 792, 1188, 0.4000, 0.7520, 0.2480, '2025-10-20'
    UNION ALL SELECT '5009', '小米路由器AX9000 WiFi6增强版 9000M速率 三频 高通6核处理器 黑色', 'shop_001', '小米官方旗舰店', '网络设备', 1520, 608, 912, 0.4000, 0.7650, 0.2350, '2025-10-20'
    UNION ALL SELECT '5010', '小米电视ES65 2023款 4K超高清 多分区背光 MEMC运动补偿 3+32GB 金属全面屏', 'shop_001', '小米官方旗舰店', '电视', 1280, 512, 768, 0.4000, 0.6450, 0.3550, '2025-10-20'
) t;


-- 5 商品评价表
DROP TABLE IF EXISTS ads_sku_comment_analysis_day;
-- 创建表（ODPS兼容语法）
CREATE TABLE IF NOT EXISTS ads_sku_comment_analysis_day (
    sku_id STRING NOT NULL COMMENT '商品SKU ID',
    sku_name STRING COMMENT '商品名称（适配含规格的长商品名，如"戴森吹风机Supersonic HD08 镍粉色"）',
    shop_id STRING COMMENT '店铺ID',
    shop_name STRING COMMENT '店铺名称',
    tm_name STRING COMMENT '品牌名称',
    total_comment_num BIGINT COMMENT '当日总有效评价数',
    good_comment_num BIGINT COMMENT '当日好评数（appraise_type=1）',
    mid_comment_num BIGINT COMMENT '当日中评数（appraise_type=2）',
    bad_comment_num BIGINT COMMENT '当日差评数（appraise_type=3）',
    avg_score DECIMAL(10,2) COMMENT '商品评分（对应指标15，评价概况核心指标）',
    good_comment_rate DECIMAL(10,4) COMMENT '当日好评率（辅助口碑评估）',
    dt STRING NOT NULL COMMENT '数据日期（对应原Hive分区字段）'
) COMMENT '商品评价分析指标日报表（含评分与好评率）'
PARTITIONED BY (ds STRING);

-- 基于现有表的插入语句
INSERT OVERWRITE TABLE ads_sku_comment_analysis_day PARTITION (ds = '2025-09-22')
SELECT
    CAST(sku.sku_id AS STRING) as sku_id,
    sku.sku_name,
    -- 店铺信息（模拟）
    'SHOP_' || CAST(ROW_NUMBER() OVER(PARTITION BY sku.sku_id ORDER BY sku.sku_id) % 10 + 1 AS STRING) as shop_id,
    '店铺_' || CAST(ROW_NUMBER() OVER(PARTITION BY sku.sku_id ORDER BY sku.sku_id) % 10 + 1 AS STRING) as shop_name,
    sku.tm_name,
    -- 评价数据（基于评论表计算）
    COUNT(DISTINCT ci.id) as total_comment_num,
    COUNT(DISTINCT CASE WHEN ci.appraise = '1' THEN ci.id END) as good_comment_num,
    COUNT(DISTINCT CASE WHEN ci.appraise = '2' THEN ci.id END) as mid_comment_num,
    COUNT(DISTINCT CASE WHEN ci.appraise = '3' THEN ci.id END) as bad_comment_num,
    -- 商品评分（加权计算）
    CAST(
        CASE
            WHEN COUNT(DISTINCT ci.id) > 0
            THEN ROUND(
                (COUNT(DISTINCT CASE WHEN ci.appraise = '1' THEN ci.id END) * 5.0 +
                 COUNT(DISTINCT CASE WHEN ci.appraise = '2' THEN ci.id END) * 3.0 +
                 COUNT(DISTINCT CASE WHEN ci.appraise = '3' THEN ci.id END) * 1.0) /
                COUNT(DISTINCT ci.id),
                2
            )
            ELSE 0.00
        END AS DECIMAL(10,2)
    ) as avg_score,
    -- 好评率
    CAST(
        CASE
            WHEN COUNT(DISTINCT ci.id) > 0
            THEN ROUND(
                COUNT(DISTINCT CASE WHEN ci.appraise = '1' THEN ci.id END) * 1.0 /
                COUNT(DISTINCT ci.id),
                4
            )
            ELSE 0.0000
        END AS DECIMAL(10,4)
    ) as good_comment_rate,
    '2025-09-22' as dt
FROM dwd_dim_sku_full sku
LEFT JOIN realtime_v1_ods_comment_info ci ON sku.sku_id = ci.sku_id AND ci.ds = '20251019'
WHERE sku.ds = '20251019'
GROUP BY
    sku.sku_id, sku.sku_name, sku.tm_name;

-- 批量插入商品评价分析数据（使用UNION ALL方式）
INSERT OVERWRITE TABLE ads_sku_comment_analysis_day PARTITION (ds)
SELECT
    CAST(sku_id AS STRING) as sku_id,
    sku_name,
    CAST(shop_id AS STRING) as shop_id,
    shop_name,
    tm_name,
    total_comment_num,
    good_comment_num,
    mid_comment_num,
    bad_comment_num,
    CAST(avg_score AS DECIMAL(10,2)) as avg_score,
    CAST(good_comment_rate AS DECIMAL(10,4)) as good_comment_rate,
    dt,
    dt as ds
FROM (
    -- 2025-10-15 数据
    SELECT 1 as sku_id, '小米12S Ultra 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 12GB+512GB 冷杉绿 5G手机' as sku_name, 17 as shop_id, '小米全品类旗舰店' as shop_name, '小米' as tm_name, 128 as total_comment_num, 115 as good_comment_num, 10 as mid_comment_num, 3 as bad_comment_num, 4.72 as avg_score, 0.8984 as good_comment_rate, '2025-10-15' as dt
    UNION ALL SELECT 1, '小米12S Ultra 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 12GB+512GB 冷杉绿 5G手机', 2, '小米12S系列专卖店', '小米', 95, 86, 7, 2, 4.80, 0.9053, '2025-10-15'
    UNION ALL SELECT 1, '小米12S Ultra 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 12GB+512GB 冷杉绿 5G手机', 1, '小米旗舰手机体验店', '小米', 156, 138, 14, 4, 4.65, 0.8846, '2025-10-15'
    -- 可以继续添加其他数据...
) t;
INSERT OVERWRITE TABLE ads_sku_comment_analysis_day PARTITION (ds = '20251020')
SELECT
    sku_id,
    sku_name,
    shop_id,
    shop_name,
    tm_name,
    total_comment_num,
    good_comment_num,
    mid_comment_num,
    bad_comment_num,
    avg_score,
    good_comment_rate,
    dt
FROM (
    -- 小米12S系列 (1-10)
    SELECT '1001' as sku_id, '小米12S Ultra 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 12GB+512GB 冷杉绿 5G手机' as sku_name, 'shop_001' as shop_id, '小米官方旗舰店' as shop_name, '小米' as tm_name, 128 as total_comment_num, 118 as good_comment_num, 8 as mid_comment_num, 2 as bad_comment_num, 4.85 as avg_score, 0.9219 as good_comment_rate, '2025-10-20' as dt
    UNION ALL SELECT '1002', '小米12S Ultra 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 12GB+256GB 经典黑 5G手机', 'shop_001', '小米官方旗舰店', '小米', 112, 103, 7, 2, 4.82, 0.9196, '2025-10-20'
    UNION ALL SELECT '1003', '小米12S Ultra 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 8GB+256GB 冷杉绿 5G手机', 'shop_001', '小米官方旗舰店', '小米', 96, 88, 6, 2, 4.79, 0.9167, '2025-10-20'
    UNION ALL SELECT '1004', '小米12S Pro 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 12GB+256GB 黑色 5G手机', 'shop_001', '小米官方旗舰店', '小米', 108, 100, 6, 2, 4.81, 0.9259, '2025-10-20'
    UNION ALL SELECT '1005', '小米12S Pro 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 8GB+256GB 银色 5G手机', 'shop_001', '小米官方旗舰店', '小米', 92, 85, 5, 2, 4.78, 0.9239, '2025-10-20'
    UNION ALL SELECT '1006', '小米12S Pro 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 8GB+128GB 蓝色 5G手机', 'shop_001', '小米官方旗舰店', '小米', 84, 77, 5, 2, 4.76, 0.9167, '2025-10-20'
    UNION ALL SELECT '1007', '小米12S 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 12GB+256GB 白色 5G手机', 'shop_001', '小米官方旗舰店', '小米', 96, 89, 5, 2, 4.80, 0.9271, '2025-10-20'
    UNION ALL SELECT '1008', '小米12S 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 8GB+256GB 黑色 5G手机', 'shop_001', '小米官方旗舰店', '小米', 88, 81, 5, 2, 4.77, 0.9205, '2025-10-20'
    UNION ALL SELECT '1009', '小米12S 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 8GB+128GB 蓝色 5G手机', 'shop_001', '小米官方旗舰店', '小米', 76, 70, 4, 2, 4.74, 0.9211, '2025-10-20'
    UNION ALL SELECT '1010', '小米12S Ultra 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 12GB+512GB 冷杉绿 5G手机', 'shop_002', '小米授权专卖店', '小米', 85, 78, 5, 2, 4.82, 0.9176, '2025-10-20'

    -- Redmi系列 (11-20)
    UNION ALL SELECT '2001', 'Redmi 10X 4G Helio G85游戏芯 4800万超清四摄 5020mAh大电量 小孔全面屏 128GB大存储 6GB+128GB 冰雾白 游戏智能手机', 'shop_003', '红米授权专卖店', 'Redmi', 152, 135, 12, 5, 4.65, 0.8882, '2025-10-20'
    UNION ALL SELECT '2002', 'Redmi 10X 4G Helio G85游戏芯 4800万超清四摄 5020mAh大电量 小孔全面屏 256GB大存储 6GB+256GB 冰雾白 游戏智能手机', 'shop_003', '红米授权专卖店', 'Redmi', 136, 121, 10, 5, 4.63, 0.8897, '2025-10-20'
    UNION ALL SELECT '2003', 'Redmi Note 12 骁龙4 Gen1 5000mAh大电量 1080P全高清屏 128GB存储 6GB+128GB 绿色 智能手机', 'shop_003', '红米授权专卖店', 'Redmi', 168, 152, 11, 5, 4.68, 0.9048, '2025-10-20'
    UNION ALL SELECT '2004', 'Redmi Note 12 Pro 天玑1080 120Hz OLED直屏 5000mAh大电量 8GB+128GB 蓝色 智能手机', 'shop_003', '红米授权专卖店', 'Redmi', 148, 134, 9, 5, 4.70, 0.9054, '2025-10-20'
    UNION ALL SELECT '2005', 'Redmi 10X 4G Helio G85游戏芯 4800万超清四摄 5020mAh大电量 小孔全面屏 128GB大存储 6GB+128GB 冰雾白 游戏智能手机', 'shop_004', '红米社区店', 'Redmi', 112, 100, 8, 4, 4.62, 0.8929, '2025-10-20'
    UNION ALL SELECT '2006', 'Redmi 10X 4G Helio G85游戏芯 4800万超清四摄 5020mAh大电量 小孔全面屏 256GB大存储 6GB+256GB 冰雾白 游戏智能手机', 'shop_004', '红米社区店', 'Redmi', 98, 87, 7, 4, 4.60, 0.8878, '2025-10-20'
    UNION ALL SELECT '2007', 'Redmi Note 12 骁龙4 Gen1 5000mAh大电量 1080P全高清屏 128GB存储 6GB+128GB 绿色 智能手机', 'shop_004', '红米社区店', 'Redmi', 124, 112, 8, 4, 4.65, 0.9032, '2025-10-20'
    UNION ALL SELECT '2008', 'Redmi Note 12 Pro 天玑1080 120Hz OLED直屏 5000mAh大电量 8GB+128GB 蓝色 智能手机', 'shop_004', '红米社区店', 'Redmi', 108, 98, 6, 4, 4.67, 0.9074, '2025-10-20'
    UNION ALL SELECT '2009', 'Redmi 10X 4G Helio G85游戏芯 4800万超清四摄 5020mAh大电量 小孔全面屏 128GB大存储 6GB+128GB 冰雾白 游戏智能手机', 'shop_005', '红米体验店', 'Redmi', 88, 78, 6, 4, 4.59, 0.8864, '2025-10-20'
    UNION ALL SELECT '2010', 'Redmi 10X 4G Helio G85游戏芯 4800万超清四摄 5020mAh大电量 小孔全面屏 256GB大存储 6GB+256GB 冰雾白 游戏智能手机', 'shop_005', '红米体验店', 'Redmi', 76, 67, 5, 4, 4.57, 0.8816, '2025-10-20'

    -- 平板电脑系列 (21-30)
    UNION ALL SELECT '3001', '小米平板6 Pro 11英寸 2.8K超清屏 骁龙8+ 144Hz高刷 8600mAh 67W快充 8GB+256GB 黑色', 'shop_001', '小米官方旗舰店', '小米', 96, 89, 5, 2, 4.78, 0.9271, '2025-10-20'
    UNION ALL SELECT '3002', '小米平板6 Pro 11英寸 2.8K超清屏 骁龙8+ 144Hz高刷 8600mAh 67W快充 12GB+512GB 银色', 'shop_001', '小米官方旗舰店', '小米', 84, 77, 5, 2, 4.75, 0.9167, '2025-10-20'
    UNION ALL SELECT '3003', '小米平板6 11英寸 2.8K超清屏 骁龙870 144Hz高刷 8840mAh 33W快充 8GB+128GB 蓝色', 'shop_001', '小米官方旗舰店', '小米', 72, 66, 4, 2, 4.72, 0.9167, '2025-10-20'
    UNION ALL SELECT '3004', '小米平板6 Pro 11英寸 2.8K超清屏 骁龙8+ 144Hz高刷 8600mAh 67W快充 8GB+256GB 黑色', 'shop_006', '小米平板专卖店', '小米', 68, 62, 4, 2, 4.74, 0.9118, '2025-10-20'
    UNION ALL SELECT '3005', '小米平板6 11英寸 2.8K超清屏 骁龙870 144Hz高刷 8840mAh 33W快充 8GB+128GB 蓝色', 'shop_006', '小米平板专卖店', '小米', 56, 51, 3, 2, 4.70, 0.9107, '2025-10-20'
    UNION ALL SELECT '3006', 'Redmi Pad SE 11英寸 90Hz 护眼屏 8000mAh 18W快充 8GB+128GB 深空灰', 'shop_003', '红米授权专卖店', 'Redmi', 84, 75, 6, 3, 4.62, 0.8929, '2025-10-20'
    UNION ALL SELECT '3007', 'Redmi Pad SE 11英寸 90Hz 护眼屏 8000mAh 18W快充 6GB+128GB 烟青绿', 'shop_003', '红米授权专卖店', 'Redmi', 72, 64, 5, 3, 4.60, 0.8889, '2025-10-20'
    UNION ALL SELECT '3008', '小米平板6 Pro 11英寸 2.8K超清屏 骁龙8+ 144Hz高刷 8600mAh 67W快充 8GB+256GB 黑色', 'shop_002', '小米授权专卖店', '小米', 52, 47, 3, 2, 4.73, 0.9038, '2025-10-20'
    UNION ALL SELECT '3009', '小米平板6 11英寸 2.8K超清屏 骁龙870 144Hz高刷 8840mAh 33W快充 8GB+128GB 蓝色', 'shop_002', '小米授权专卖店', '小米', 44, 39, 3, 2, 4.68, 0.8864, '2025-10-20'
    UNION ALL SELECT '3010', 'Redmi Pad SE 11英寸 90Hz 护眼屏 8000mAh 18W快充 8GB+128GB 深空灰', 'shop_004', '红米社区店', 'Redmi', 64, 56, 5, 3, 4.59, 0.8750, '2025-10-20'

    -- 智能穿戴设备 (31-40)
    UNION ALL SELECT '4001', '小米手表S3 1.43英寸AMOLED屏 独立通话 血氧心率监测 12天续航 蓝牙版 黑色', 'shop_001', '小米官方旗舰店', '小米', 112, 104, 6, 2, 4.76, 0.9286, '2025-10-20'
    UNION ALL SELECT '4002', '小米手表S3 1.43英寸AMOLED屏 独立通话 血氧心率监测 12天续航 蓝牙版 银色', 'shop_001', '小米官方旗舰店', '小米', 98, 90, 6, 2, 4.74, 0.9184, '2025-10-20'
    UNION ALL SELECT '4003', '小米手环8 Pro 1.74英寸AMOLED大屏 独立GPS 血氧心率监测 14天续航 黑色', 'shop_001', '小米官方旗舰店', '小米', 156, 148, 6, 2, 4.82, 0.9487, '2025-10-20'
    UNION ALL SELECT '4004', '小米手环8 Pro 1.74英寸AMOLED大屏 独立GPS 血氧心率监测 14天续航 白色', 'shop_001', '小米官方旗舰店', '小米', 144, 136, 6, 2, 4.80, 0.9444, '2025-10-20'
    UNION ALL SELECT '4005', '小米手表S3 1.43英寸AMOLED屏 独立通话 血氧心率监测 12天续航 蓝牙版 黑色', 'shop_007', '小米穿戴专卖店', '小米', 76, 70, 4, 2, 4.72, 0.9211, '2025-10-20'
    UNION ALL SELECT '4006', '小米手环8 Pro 1.74英寸AMOLED大屏 独立GPS 血氧心率监测 14天续航 黑色', 'shop_007', '小米穿戴专卖店', '小米', 112, 106, 4, 2, 4.79, 0.9464, '2025-10-20'
    UNION ALL SELECT '4007', 'Redmi Watch 4 1.97英寸AMOLED屏 独立GPS 血氧监测 10天续航 黑色', 'shop_003', '红米授权专卖店', 'Redmi', 92, 84, 6, 2, 4.65, 0.9130, '2025-10-20'
    UNION ALL SELECT '4008', 'Redmi Watch 4 1.97英寸AMOLED屏 独立GPS 血氧监测 10天续航 白色', 'shop_003', '红米授权专卖店', 'Redmi', 84, 76, 6, 2, 4.63, 0.9048, '2025-10-20'
    UNION ALL SELECT '4009', '小米手表S3 1.43英寸AMOLED屏 独立通话 血氧心率监测 12天续航 蓝牙版 黑色', 'shop_002', '小米授权专卖店', '小米', 56, 51, 3, 2, 4.70, 0.9107, '2025-10-20'
    UNION ALL SELECT '4010', '小米手环8 Pro 1.74英寸AMOLED大屏 独立GPS 血氧心率监测 14天续航 黑色', 'shop_002', '小米授权专卖店', '小米', 68, 64, 3, 1, 4.78, 0.9412, '2025-10-20'

    -- 音频设备和配件 (41-50)
    UNION ALL SELECT '5001', '小米蓝牙耳机Air 3 Pro 主动降噪 无线充电 超长续航 28小时 白色', 'shop_001', '小米官方旗舰店', '小米', 168, 158, 7, 3, 4.78, 0.9405, '2025-10-20'
    UNION ALL SELECT '5002', '小米蓝牙耳机Air 3 Pro 主动降噪 无线充电 超长续航 28小时 黑色', 'shop_001', '小米官方旗舰店', '小米', 152, 142, 7, 3, 4.76, 0.9342, '2025-10-20'
    UNION ALL SELECT '5003', 'Redmi Buds 5 Pro 主动降噪 无线充电 40小时续航 白色', 'shop_003', '红米授权专卖店', 'Redmi', 128, 118, 7, 3, 4.72, 0.9219, '2025-10-20'
    UNION ALL SELECT '5004', 'Redmi Buds 5 Pro 主动降噪 无线充电 40小时续航 黑色', 'shop_003', '红米授权专卖店', 'Redmi', 116, 106, 7, 3, 4.70, 0.9138, '2025-10-20'
    UNION ALL SELECT '5005', '小米充电宝10000mAh 22.5W快充 Type-C接口 双向快充 白色', 'shop_001', '小米官方旗舰店', '小米', 224, 216, 6, 2, 4.85, 0.9643, '2025-10-20'
    UNION ALL SELECT '5006', '小米充电宝20000mAh 50W快充 Type-C接口 双向快充 黑色', 'shop_001', '小米官方旗舰店', '小米', 196, 188, 6, 2, 4.83, 0.9592, '2025-10-20'
    UNION ALL SELECT '5007', '小米充电宝10000mAh 22.5W快充 Type-C接口 双向快充 白色', 'shop_009', '小米配件店', '小米', 152, 146, 4, 2, 4.82, 0.9605, '2025-10-20'
    UNION ALL SELECT '5008', '小米路由器AX6000 WiFi6 6000M速率 8数据流 6核处理器 黑色', 'shop_001', '小米官方旗舰店', '小米', 98, 90, 6, 2, 4.74, 0.9184, '2025-10-20'
    UNION ALL SELECT '5009', '小米路由器AX9000 WiFi6增强版 9000M速率 三频 高通6核处理器 黑色', 'shop_001', '小米官方旗舰店', '小米', 84, 77, 5, 2, 4.76, 0.9167, '2025-10-20'
    UNION ALL SELECT '5010', '小米电视ES65 2023款 4K超高清 多分区背光 MEMC运动补偿 3+32GB 金属全面屏', 'shop_001', '小米官方旗舰店', '小米', 72, 65, 5, 2, 4.72, 0.9028, '2025-10-20'
) t;


SELECT * FROM `ads_sku_conversion_sales_day` sku left join
              ads_sku_user_interaction_day user on sku.sku_id = user.sku_id
                                                 left join dwd_order_detail od on sku.sku_id = od.sku_id

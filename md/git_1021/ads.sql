-- USE df_cb_323768;

DROP TABLE IF EXISTS ads_sku_full_metrics_with_price;

CREATE TABLE ads_sku_full_metrics_with_price (
    ds STRING COMMENT '统计日期（分区字段）',
    store_no STRING COMMENT '店铺编号',
    product_id STRING COMMENT '商品ID',

    sku_uv BIGINT COMMENT '商品访问人数（UV）',
    sku_pv BIGINT COMMENT '商品访问次数（PV）',

    active_product_cnt BIGINT COMMENT '活跃商品数量',
    avg_stay_seconds DOUBLE COMMENT '平均停留时长（秒）',
    detail_page_bounce_rate DOUBLE COMMENT '详情页跳出率',

    favor_user_cnt BIGINT COMMENT '收藏用户数',
    cart_user_cnt BIGINT COMMENT '加购用户数',
    cart_quantity BIGINT COMMENT '加购数量',

    cart_conversion_rate DOUBLE COMMENT '加购转化率（加购用户数 / UV）',
    favor_conversion_rate DOUBLE COMMENT '收藏转化率（收藏用户数 / UV）',

    pay_user_cnt BIGINT COMMENT '支付用户数',
    pay_quantity BIGINT COMMENT '支付件数',
    pay_amount DOUBLE COMMENT '支付金额',

    pay_product_cnt BIGINT COMMENT '支付商品数（去重）',
    pay_conversion_rate DOUBLE COMMENT '支付转化率（支付用户数 / UV）',

    price_range STRING COMMENT '价格区间（0-99 / 100-199 / 200-499 / 500-999 / 1000+）',
    price_range_pay_quantity BIGINT COMMENT '该价格区间支付件数',
    price_range_pay_amount DOUBLE COMMENT '该价格区间支付金额'
)
COMMENT '商品全量指标宽表（含价格区间统计）'
;

INSERT OVERWRITE TABLE ads_sku_full_metrics_with_price
SELECT
    m.ds,
    m.store_no,
    m.product_id,
    m.uv AS sku_uv,
    m.pv_cnt AS sku_pv,
    ups.active_product_cnt,
    ups.avg_stay_seconds,
    ups.detail_page_bounce_rate,
    m.favor_user_cnt,
    m.cart_user_cnt,
    m.cart_quantity,
    ROUND(m.cart_user_cnt / NULLIF(m.uv, 0), 4) AS cart_conversion_rate,
    ROUND(m.favor_user_cnt / NULLIF(m.uv, 0), 4) AS favor_conversion_rate,
    m.pay_user_cnt,
    m.pay_quantity,
    m.pay_amount,
    pp.pay_product_cnt,
    ROUND(m.pay_user_cnt / NULLIF(m.uv, 0), 4) AS pay_conversion_rate,
    pb.price_range,
    SUM(pb.pay_quantity) AS price_range_pay_quantity,
    SUM(pb.pay_amount) AS price_range_pay_amount
FROM dws_sku_metrics m
LEFT JOIN (
    SELECT
        ds, store_no, product_id,
        COUNT(DISTINCT product_id) AS active_product_cnt,
        AVG(
            CAST(
                UNIX_TIMESTAMP(last_visit, 'yyyy-MM-dd HH:mm:ss')
                - UNIX_TIMESTAMP(first_visit, 'yyyy-MM-dd HH:mm:ss')
            AS DOUBLE)
        ) AS avg_stay_seconds,
        SUM(CASE WHEN visit_count = 1 THEN 1 ELSE 0 END) / COUNT(*) AS detail_page_bounce_rate
    FROM (
        SELECT
            ds, store_no, product_id, userid,
            MIN(event_time) AS first_visit,
            MAX(event_time) AS last_visit,
            COUNT(*) AS visit_count
        FROM dwd_sku_pv
        WHERE ds = '20251020'
        GROUP BY ds, store_no, product_id, userid
    ) t
    GROUP BY ds, store_no, product_id
) ups
ON m.ds=ups.ds AND m.store_no=ups.store_no AND m.product_id=ups.product_id

LEFT JOIN (
    SELECT ds, store_no, product_id,
           COUNT(DISTINCT product_id) AS pay_product_cnt
    FROM dwd_sku_order
    WHERE ds='20251020'
    GROUP BY ds, store_no, product_id
) pp
ON m.ds=pp.ds AND m.store_no=pp.store_no AND m.product_id=pp.product_id

LEFT JOIN (
    SELECT
        ds, store_no, product_id, price_range,
        SUM(quantity) AS pay_quantity,
        SUM(price * quantity) AS pay_amount
    FROM (
        SELECT
            ds, store_no, product_id,
            quantity, price,
            CASE
                WHEN price * quantity BETWEEN 0 AND 99 THEN '0-99'
                WHEN price * quantity BETWEEN 100 AND 199 THEN '100-199'
                WHEN price * quantity BETWEEN 200 AND 499 THEN '200-499'
                WHEN price * quantity BETWEEN 500 AND 999 THEN '500-999'
                ELSE '1000+'
            END AS price_range
        FROM dwd_sku_order
        WHERE ds='20251020'
    ) tmp
    GROUP BY ds, store_no, product_id, price_range
) pb
ON m.ds=pb.ds AND m.store_no=pb.store_no AND m.product_id=pb.product_id

WHERE m.ds='20251020'
GROUP BY
    m.ds, m.store_no, m.product_id,
    ups.active_product_cnt, ups.avg_stay_seconds,
    ups.detail_page_bounce_rate, m.uv, m.pv_cnt,
    m.favor_user_cnt, m.cart_user_cnt, m.cart_quantity,
    m.pay_user_cnt, m.pay_quantity, m.pay_amount,
    pp.pay_product_cnt, pb.price_range;

INSERT INTO TABLE ads_sku_full_metrics_with_price
VALUES
-- ds, store_no, product_id, sku_uv, sku_pv, active_product_cnt, avg_stay_seconds, detail_page_bounce_rate,
-- favor_user_cnt, cart_user_cnt, cart_quantity, cart_conversion_rate, favor_conversion_rate,
-- pay_user_cnt, pay_quantity, pay_amount, pay_product_cnt, pay_conversion_rate,
-- price_range, price_range_pay_quantity, price_range_pay_amount
('20251020', 'S001', 'P001', 1000, 1500, 80, 35.2, 0.25, 300, 250, 400, 0.25, 0.3, 180, 350, 32000.5, 60, 0.18, '0-99', 50, 2500.5),
('20251020', 'S001', 'P002', 900, 1400, 70, 38.1, 0.22, 250, 210, 320, 0.23, 0.27, 160, 290, 27500.0, 55, 0.18, '100-199', 45, 6300.0),
('20251020', 'S001', 'P003', 1200, 1800, 90, 40.5, 0.20, 350, 310, 500, 0.26, 0.29, 200, 410, 38000.0, 70, 0.17, '200-499', 60, 15000.0),
('20251020', 'S001', 'P004', 800, 1100, 60, 32.8, 0.27, 220, 180, 280, 0.23, 0.28, 140, 250, 22000.0, 48, 0.18, '500-999', 40, 21000.0),
('20251020', 'S001', 'P005', 1500, 2100, 95, 45.6, 0.19, 400, 360, 520, 0.24, 0.27, 220, 430, 40000.0, 75, 0.15, '1000+', 65, 70000.0),
('20251020', 'S002', 'P006', 700, 950, 50, 30.3, 0.30, 180, 160, 250, 0.23, 0.26, 120, 200, 19000.0, 40, 0.17, '0-99', 35, 2600.0),
('20251020', 'S002', 'P007', 850, 1200, 65, 33.5, 0.28, 210, 190, 280, 0.22, 0.25, 140, 250, 22500.0, 50, 0.16, '100-199', 45, 6700.0),
('20251020', 'S002', 'P008', 1000, 1450, 75, 36.8, 0.23, 260, 230, 330, 0.23, 0.26, 160, 300, 27000.0, 55, 0.16, '200-499', 50, 16500.0),
('20251020', 'S002', 'P009', 950, 1400, 72, 37.9, 0.21, 240, 220, 310, 0.23, 0.25, 150, 280, 26000.0, 52, 0.16, '500-999', 48, 22000.0),
('20251020', 'S002', 'P010', 1100, 1600, 80, 39.7, 0.19, 300, 270, 360, 0.24, 0.27, 170, 320, 30000.0, 60, 0.15, '1000+', 58, 75000.0),
('20251020', 'S003', 'P011', 900, 1250, 68, 34.2, 0.26, 230, 200, 280, 0.22, 0.26, 140, 260, 24000.0, 50, 0.16, '0-99', 45, 2900.0),
('20251020', 'S003', 'P012', 1050, 1500, 78, 37.0, 0.22, 270, 240, 310, 0.23, 0.26, 160, 290, 27500.0, 55, 0.15, '100-199', 50, 6200.0),
('20251020', 'S003', 'P013', 1150, 1650, 83, 38.4, 0.21, 290, 260, 340, 0.23, 0.25, 170, 310, 29500.0, 58, 0.15, '200-499', 55, 17500.0),
('20251020', 'S003', 'P014', 980, 1350, 70, 35.5, 0.24, 250, 220, 300, 0.22, 0.25, 150, 280, 25500.0, 53, 0.15, '500-999', 50, 23000.0),
('20251020', 'S003', 'P015', 1300, 1850, 90, 42.0, 0.18, 350, 310, 420, 0.24, 0.27, 190, 350, 34000.0, 65, 0.15, '1000+', 60, 82000.0),
('20251020', 'S004', 'P016', 870, 1200, 65, 33.1, 0.27, 220, 190, 270, 0.22, 0.25, 130, 240, 21000.0, 48, 0.15, '0-99', 40, 2700.0),
('20251020', 'S004', 'P017', 940, 1300, 70, 34.7, 0.25, 240, 210, 290, 0.22, 0.25, 140, 260, 23000.0, 50, 0.15, '100-199', 45, 6500.0),
('20251020', 'S004', 'P018', 1020, 1450, 75, 36.3, 0.22, 260, 230, 320, 0.23, 0.26, 160, 290, 26500.0, 54, 0.16, '200-499', 48, 18000.0),
('20251020', 'S004', 'P019', 980, 1380, 72, 35.9, 0.23, 240, 220, 300, 0.23, 0.25, 150, 280, 25000.0, 52, 0.15, '500-999', 45, 22500.0),
('20251020', 'S004', 'P020', 1180, 1700, 85, 40.8, 0.19, 320, 280, 400, 0.24, 0.27, 180, 340, 32000.0, 62, 0.15, '1000+', 58, 78000.0),
-- 30 more rows (重复模式自动变化)
('20251020', 'S005', 'P021', 1000, 1450, 75, 36.0, 0.23, 260, 230, 330, 0.23, 0.26, 160, 300, 27000.0, 55, 0.16, '0-99', 45, 2500.0),
('20251020', 'S005', 'P022', 1050, 1500, 78, 37.5, 0.21, 270, 240, 340, 0.24, 0.27, 170, 310, 28500.0, 56, 0.16, '100-199', 50, 6300.0),
('20251020', 'S005', 'P023', 1150, 1650, 83, 38.8, 0.20, 290, 260, 350, 0.24, 0.27, 180, 320, 29500.0, 58, 0.16, '200-499', 55, 17000.0),
('20251020', 'S005', 'P024', 950, 1350, 72, 35.4, 0.24, 250, 220, 310, 0.23, 0.26, 150, 280, 25500.0, 53, 0.15, '500-999', 50, 22500.0),
('20251020', 'S005', 'P025', 1300, 1850, 90, 41.9, 0.18, 350, 310, 420, 0.24, 0.27, 190, 350, 34000.0, 65, 0.15, '1000+', 60, 82000.0),
('20251020', 'S006', 'P026', 870, 1200, 65, 33.2, 0.27, 220, 190, 270, 0.22, 0.25, 130, 240, 21000.0, 48, 0.15, '0-99', 40, 2700.0),
('20251020', 'S006', 'P027', 940, 1300, 70, 34.8, 0.25, 240, 210, 290, 0.22, 0.25, 140, 260, 23000.0, 50, 0.15, '100-199', 45, 6500.0),
('20251020', 'S006', 'P028', 1020, 1450, 75, 36.4, 0.22, 260, 230, 320, 0.23, 0.26, 160, 290, 26500.0, 54, 0.16, '200-499', 48, 18000.0),
('20251020', 'S006', 'P029', 980, 1380, 72, 36.0, 0.23, 240, 220, 300, 0.23, 0.25, 150, 280, 25000.0, 52, 0.15, '500-999', 45, 22500.0),
('20251020', 'S006', 'P030', 1180, 1700, 85, 41.0, 0.19, 320, 280, 400, 0.24, 0.27, 180, 340, 32000.0, 62, 0.15, '1000+', 58, 78000.0);

select *
from ads_sku_full_metrics_with_price
where ds='20251020';

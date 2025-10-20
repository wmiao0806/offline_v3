-- 1. 创建目标 result 表（若不存在）
CREATE TABLE IF NOT EXISTS ads_product_result_dtl (
  dt STRING,               -- 业务日期（例如 2025-10-17）
  shop_no STRING,
  stat_hour STRING,        -- 统计粒度时间（小时粒度的时间标记），例如 '2025-10-17 14:00:00'
  product_id STRING,
  product_pv BIGINT,
  product_uv BIGINT,
  avg_duration DOUBLE,
  bounce_rate DOUBLE,
  fav_users BIGINT,
  cart_qty BIGINT,
  cart_users BIGINT,
  visit_to_fav_rate DOUBLE,
  visit_to_cart_rate DOUBLE,
  order_users BIGINT,
  order_qty BIGINT,
  order_amount DOUBLE,
  order_rate DOUBLE,
  pay_users BIGINT,
  pay_qty BIGINT,
  pay_amount DOUBLE,
  pay_rate DOUBLE
)
PARTITIONED BY (dt STRING)
STORED AS PARQUET;

-- 如果按小时运行，bizdate 传入 yyyy-MM-dd
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;


-- 2. 生成中间表 t1：来自 ods_logs —— 计算 pv, uv, avg_duration, bounce_rate, product访客数、浏览量、停留时长
WITH pv_base AS (
  SELECT
    shop_no,
    product_id,
    date_format(event_time,'yyyy-MM-dd HH:00:00') AS stat_hour,
    COUNT(1) AS product_pv,
    COUNT(DISTINCT user_id) AS product_uv,
    AVG(duration_seconds) AS avg_duration,
    SUM(CASE WHEN is_bounce=1 THEN 1 ELSE 0 END) AS bounce_cnt
  FROM ods_logs_jdbc
  WHERE event_time >= date_sub(current_date, 30)  -- 仅示例：过去30天
  GROUP BY shop_no, product_id, date_format(event_time,'yyyy-MM-dd HH:00:00')
),
pv_final AS (
  SELECT
    shop_no,
    product_id,
    stat_hour,
    product_pv,
    product_uv,
    avg_duration,
    CASE WHEN product_pv = 0 THEN 0 ELSE (cast(bounce_cnt as double)/product_pv) END AS bounce_rate
  FROM pv_base
)

-- 3. t2：收藏 & 加购（按小时聚合）
, fav_agg AS (
  SELECT
    shop_no,
    product_id,
    date_format(favor_time,'yyyy-MM-dd HH:00:00') AS stat_hour,
    COUNT(DISTINCT user_id) AS fav_users
  FROM dim_favorite_jdbc
  GROUP BY shop_no, product_id, date_format(favor_time,'yyyy-MM-dd HH:00:00')
),
cart_agg AS (
  SELECT
    shop_no,
    product_id,
    date_format(add_time,'yyyy-MM-dd HH:00:00') AS stat_hour,
    SUM(qty) AS cart_qty,
    COUNT(DISTINCT user_id) AS cart_users
  FROM dim_cart_jdbc
  GROUP BY shop_no, product_id, date_format(add_time,'yyyy-MM-dd HH:00:00')
)

-- 4. t3：下单/支付相关（order_info + order_detail）
, order_join AS (
  SELECT
    od.product_id,
    oi.shop_no,
    date_format(oi.order_time,'yyyy-MM-dd HH:00:00') AS stat_hour,
    SUM(od.qty) AS order_qty,
    SUM(od.amount) AS order_amount,
    COUNT(DISTINCT CASE WHEN oi.status IN ('CREATED','PAID') THEN oi.user_id END) AS order_users
  FROM order_info_jdbc oi
  JOIN order_detail_jdbc od ON oi.id = od.order_id
  GROUP BY od.product_id, oi.shop_no, date_format(oi.order_time,'yyyy-MM-dd HH:00:00')
),
pay_join AS (
  SELECT
    od.product_id,
    oi.shop_no,
    date_format(oi.pay_time,'yyyy-MM-dd HH:00:00') AS stat_hour,
    SUM(od.qty) AS pay_qty,
    SUM(od.amount) AS pay_amount,
    COUNT(DISTINCT CASE WHEN oi.status='PAID' THEN oi.user_id END) AS pay_users
  FROM order_info_jdbc oi
  JOIN order_detail_jdbc od ON oi.id = od.order_id
  WHERE oi.status='PAID' AND oi.pay_time IS NOT NULL
  GROUP BY od.product_id, oi.shop_no, date_format(oi.pay_time,'yyyy-MM-dd HH:00:00')
)

-- 5. 最终汇总 join t1/t2/t3 并写入结果表
INSERT OVERWRITE TABLE ads_product_result_dtl PARTITION(dt='${bizdate}')
SELECT
  '${bizdate}' AS dt,
  coalesce(pv.shop_no, f.shop_no, c.shop_no, o.shop_no, p.shop_no) AS shop_no,
  coalesce(pv.stat_hour, f.stat_hour, c.stat_hour, o.stat_hour, p.stat_hour) AS stat_hour,
  coalesce(pv.product_id, f.product_id, c.product_id, o.product_id, p.product_id) AS product_id,
  coalesce(pv.product_pv,0) AS product_pv,
  coalesce(pv.product_uv,0) AS product_uv,
  coalesce(pv.avg_duration,0.0) AS avg_duration,
  coalesce(pv.bounce_rate,0.0) AS bounce_rate,
  coalesce(f.fav_users,0) AS fav_users,
  coalesce(c.cart_qty,0) AS cart_qty,
  coalesce(c.cart_users,0) AS cart_users,
  CASE WHEN coalesce(pv.product_uv,0)=0 THEN 0.0 ELSE round(coalesce(f.fav_users,0)/cast(pv.product_uv as double),6) END AS visit_to_fav_rate,
  CASE WHEN coalesce(pv.product_uv,0)=0 THEN 0.0 ELSE round(coalesce(c.cart_users,0)/cast(pv.product_uv as double),6) END AS visit_to_cart_rate,
  coalesce(o.order_users,0) AS order_users,
  coalesce(o.order_qty,0) AS order_qty,
  coalesce(o.order_amount,0.0) AS order_amount,
  CASE WHEN coalesce(pv.product_uv,0)=0 THEN 0.0 ELSE round(coalesce(o.order_users,0)/cast(pv.product_uv as double),6) END AS order_rate,
  coalesce(p.pay_users,0) AS pay_users,
  coalesce(p.pay_qty,0) AS pay_qty,
  coalesce(p.pay_amount,0.0) AS pay_amount,
  CASE WHEN coalesce(pv.product_uv,0)=0 THEN 0.0 ELSE round(coalesce(p.pay_users,0)/cast(pv.product_uv as double),6) END AS pay_rate
FROM pv_final pv
FULL OUTER JOIN fav_agg f
  ON pv.shop_no = f.shop_no AND pv.product_id = f.product_id AND pv.stat_hour = f.stat_hour
FULL OUTER JOIN cart_agg c
  ON coalesce(pv.shop_no,f.shop_no) = c.shop_no AND coalesce(pv.product_id,f.product_id) = c.product_id AND coalesce(pv.stat_hour,f.stat_hour) = c.stat_hour
FULL OUTER JOIN order_join o
  ON coalesce(pv.shop_no,f.shop_no,c.shop_no) = o.shop_no AND coalesce(pv.product_id,f.product_id,c.product_id) = o.product_id AND coalesce(pv.stat_hour,f.stat_hour,c.stat_hour) = o.stat_hour
FULL OUTER JOIN pay_join p
  ON coalesce(pv.shop_no,f.shop_no,c.shop_no,o.shop_no) = p.shop_no AND coalesce(pv.product_id,f.product_id,c.product_id,o.product_id) = p.product_id AND coalesce(pv.stat_hour,f.stat_hour,c.stat_hour,o.stat_hour) = p.stat_hour;

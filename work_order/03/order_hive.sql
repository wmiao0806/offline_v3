-- 商品访客数
SELECT product_id, COUNT(DISTINCT user_id) AS visitor_count
FROM ecommerce_data
GROUP BY product_id;

-- 商品浏览量
SELECT product_id, COUNT(*) AS page_views
FROM ecommerce_data
WHERE action = 'visit'
GROUP BY product_id;

-- 商品加购件数
SELECT product_id, COUNT(*) AS add_to_cart_count
FROM ecommerce_data
WHERE action = 'add_to_cart'
GROUP BY product_id;

-- 商品加购人数
SELECT product_id, COUNT(DISTINCT user_id) AS add_to_cart_users
FROM ecommerce_data
WHERE action = 'add_to_cart'
GROUP BY product_id;

-- 支付转化率
WITH purchase_data AS (
    SELECT user_id, product_id
    FROM ecommerce_data
    WHERE action = 'purchase'
),
visit_data AS (
    SELECT user_id, product_id
    FROM ecommerce_data
    WHERE action = 'visit'
)
SELECT v.product_id, 
       COUNT(DISTINCT v.user_id) AS visitors, 
       COUNT(DISTINCT p.user_id) AS purchasers,
       (COUNT(DISTINCT p.user_id) / COUNT(DISTINCT v.user_id)) * 100 AS conversion_rate
FROM visit_data v
LEFT JOIN purchase_data p ON v.user_id = p.user_id AND v.product_id = p.product_id
GROUP BY v.product_id;

-- 价格带分析
SELECT 
    CASE 
        WHEN price < 100 THEN '0-99'
        WHEN price BETWEEN 100 AND 200 THEN '100-199'
        WHEN price BETWEEN 200 AND 300 THEN '200-299'
        WHEN price BETWEEN 300 AND 400 THEN '300-399'
        WHEN price BETWEEN 400 AND 500 THEN '400-499'
        WHEN price BETWEEN 500 AND 600 THEN '500-599'
        WHEN price BETWEEN 600 AND 700 THEN '600-699'
        WHEN price BETWEEN 700 AND 800 THEN '700-799'
        WHEN price BETWEEN 800 AND 900 THEN '800-899'
        WHEN price BETWEEN 900 AND 1000 THEN '900-999'
        ELSE '1000+'
    END AS price_band,
    COUNT(*) AS purchase_count,
    SUM(price) AS total_revenue
FROM ecommerce_data
WHERE action = 'purchase'
GROUP BY price_band;

-- 流量来源分析
SELECT channel, 
       COUNT(*) AS total_visits, 
       COUNT(CASE WHEN action = 'purchase' THEN 1 END) AS total_purchases,
       (COUNT(CASE WHEN action = 'purchase' THEN 1 END) / COUNT(*)) * 100 AS conversion_rate
FROM ecommerce_data
GROUP BY channel;

-- 评价分析
SELECT product_id, 
       rating, 
       COUNT(*) AS rating_count
FROM ecommerce_data
GROUP BY product_id, rating;
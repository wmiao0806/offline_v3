-- use df_cb_323768.df_cb_323768;
-- 2.1 商品维度 dim_sku
CREATE TABLE IF NOT EXISTS dim_sku (
    product_id STRING,
    spu_id STRING,
    product_name STRING,
    brand_id STRING,
    category_id STRING,
    price DOUBLE
)
;

-- 2.2 店铺维度 dim_store
CREATE TABLE IF NOT EXISTS dim_store (
    store_no STRING,
    store_name STRING,
    region STRING
);

-- dim_sku
INSERT OVERWRITE TABLE dim_sku
SELECT
    CONCAT('product', CAST(seq AS STRING)) AS product_id,
    CONCAT('spu', CAST(seq AS STRING)) AS spu_id,
    CONCAT('商品', CAST(seq AS STRING)) AS product_name,
    CONCAT('brand', CAST(FLOOR(RAND()*20)+1 AS STRING)) AS brand_id,
    CONCAT('cat', CAST(FLOOR(RAND()*10)+1 AS STRING)) AS category_id,
    ROUND(RAND()*1000,2) AS price
FROM (SELECT explode(sequence(1,100)) as seq) t;

-- dim_store
INSERT OVERWRITE TABLE dim_store
SELECT
    CONCAT('store', CAST(seq AS STRING)) AS store_no,
    CONCAT('店铺', CAST(seq AS STRING)) AS store_name,
    CONCAT('region', CAST(FLOOR(RAND()*5)+1 AS STRING)) AS region
FROM (SELECT explode(sequence(1,10)) as seq) t;
SET odps.sql.allow.fullscan=true;
select *
from dim_sku;
-- 创建Hive表
CREATE TABLE ecommerce_data (
    user_id INT,
    product_id INT,
    sku_id INT,
    visit_time STRING,
    action STRING,
    price DECIMAL(10, 2),
    channel STRING,
    user_type STRING,
    rating INT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- 加载数据
LOAD DATA LOCAL INPATH '/path/to/ecommerce_data.csv' INTO TABLE ecommerce_data;
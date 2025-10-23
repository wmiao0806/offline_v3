CREATE TABLE ecommerce_data (
    user_id INT,
    product_id INT,
    sku_id INT,
    visit_time DATETIME,
    action VARCHAR(50),
    price DECIMAL(10, 2),
    channel VARCHAR(50),
    user_type VARCHAR(50),
    rating INT
);
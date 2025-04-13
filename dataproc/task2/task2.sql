CREATE TABLE orders
(
    order_id Int32,
    user_id Int32,
    order_date DateTime,
    total_amount Decimal(19,2),
    payment_status String
)
ENGINE = MergeTree
ORDER BY order_id;

INSERT INTO orders
SELECT *
FROM s3('https://storage.yandexcloud.net/dataproc-base-bucket/orders/orders.csv', 'CSV');

CREATE TABLE order_items
(
    item_id Int32,
    order_id Int32,
    product_name String,
    product_price Decimal(19,2),
    quantity Int32
)
ENGINE = MergeTree
ORDER BY item_id;

INSERT INTO order_items
SELECT *
FROM s3('https://storage.yandexcloud.net/dataproc-base-bucket/order_items/order_items.txt', 'CSV')
SETTINGS format_csv_delimiter = ';';

-- Query 1
SELECT
    payment_status,
    COUNT(*) AS order_cnt,
    SUM(total_amount) AS total_orders_amount,
    AVG(total_amount) AS avg_order_amount
FROM orders
GROUP BY payment_status

-- Query 2
SELECT
   SUM(quantity) AS product_quantity,
   SUM(product_price * quantity) AS total_product_amount,
   AVG(product_price) AS avg_product_price
FROM orders AS o
JOIN order_items AS oi ON o.order_id = oi.order_id

-- Query 3
SELECT
    date_trunc('day', order_date) AS order_date,
    COUNT(*) AS order_cnt,
    SUM(total_amount) AS total_order_amount
FROM orders
GROUP BY order_date

-- Query 4
SELECT
    user_id,
    SUM(total_amount) AS total_order_amount
FROM orders
GROUP BY user_id
ORDER BY total_order_amount DESC

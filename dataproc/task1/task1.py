
spark.sql("CREATE DATABASE hw")

spark.sql("USE hw")

spark.sql("""
    CREATE TABLE transactions_v2
    (
        transaction_id INT,
        user_id INT,
        amount DECIMAL,
        currency STRING,
        transaction_date TIMESTAMP,
        is_fraud INT
    )
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
""")

spark.sql("""
    CREATE TABLE logs_v2
    (
        log_id INT,
        transaction_id INT,
        category STRING,
        comment STRING,
        log_timestamp TIMESTAMP
    )
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
""")

# Read the data stored as CSV from the bucket 
transactions_v2 = spark.read.options(delimiter=",", header=True).csv("s3a://dataproc-base-bucket/transactions_v2/")
logs_v2 = spark.read.options(delimiter=";", header=True).csv("s3a://dataproc-base-bucket/logs_v2/")

# Upload the data to a Hive table
transactions_v2.write.mode('overwrite').saveAsTable("transactions_v2")
logs_v2.write.mode('overwrite').saveAsTable("logs_v2")

# Query 1
spark.sql("""
    SELECT
        currency,
        SUM(amount) AS amount
    FROM hw.transactions_v2
    WHERE currency IN ('USD', 'EUR', 'RUB')
    GROUP BY currency
""").show()

# +--------+--------+                                                             
# |currency|  amount|
# +--------+--------+
# |     RUB|  320.25|
# |     EUR|  2171.0|
# |     USD|12263.99|
# +--------+--------+

# Query 2
spark.sql("""
    SELECT
        SUM(amount) AS total_amount,
        AVG(amount) AS avg_amount,
        COUNT(*) - SUM(is_fraud) AS fine_operations,
        SUM(is_fraud) AS fraud_operations
    FROM hw.transactions_v2
""").show()

# +------------+----------+---------------+----------------+
# |total_amount|avg_amount|fine_operations|fraud_operations|
# +------------+----------+---------------+----------------+
# |    15824.19|  791.2095|           10.0|            10.0|
# +------------+----------+---------------+----------------+

# Query 3

spark.sql("""
    SELECT
        TO_DATE(transaction_date) AS transaction_date,
        COUNT(*) AS transaction_cnt,
        SUM(amount) AS transaction_amount,
        AVG(amount) AS avg_transaction_amount
    FROM hw.transactions_v2
    GROUP BY TO_DATE(transaction_date)
""").show()

# +----------------+---------------+------------------+----------------------+    
# |transaction_date|transaction_cnt|transaction_amount|avg_transaction_amount|
# +----------------+---------------+------------------+----------------------+
# |      2023-03-12|              4|            895.75|              223.9375|
# |      2023-03-14|              4|            1374.5|               343.625|
# |      2023-03-13|              4|          10743.94|              2685.985|
# |      2023-03-11|              4|            1869.5|               467.375|
# |      2023-03-10|              4|             940.5|               235.125|
# +----------------+---------------+------------------+----------------------+

# Query 4

spark.sql("""
    SELECT
        t.transaction_id,
        t.transaction_date,
        COUNT(l.log_id) AS logs_on_transaction
    FROM hw.transactions_v2 t
    LEFT JOIN hw.logs_v2 l ON t.transaction_id = l.transaction_id
    GROUP BY
        t.transaction_id,
        t.transaction_date
""").show()

# +--------------+-------------------+-------------------+
# |transaction_id|   transaction_date|logs_on_transaction|
# +--------------+-------------------+-------------------+
# |         10020|2023-03-14 10:20:00|                  1|
# |         10010|2023-03-12 11:05:00|                  2|
# |         10005|2023-03-11 09:00:00|                  0|
# |         10002|2023-03-10 14:30:00|                  1|
# |         10008|2023-03-11 10:20:00|                  0|
# |         10012|2023-03-12 11:15:00|                  0|
# |         10001|2023-03-10 14:25:00|                  1|
# |         10016|2023-03-13 09:15:00|                  0|
# |         10014|2023-03-13 09:05:00|                  1|
# |         10009|2023-03-12 11:00:00|                  0|
# |         10011|2023-03-12 11:10:00|                  0|
# |         10006|2023-03-11 09:05:00|                  1|
# |         10007|2023-03-11 10:10:00|                  1|
# |         10015|2023-03-13 09:10:00|                  1|
# |         10013|2023-03-13 09:00:00|                  1|
# |         10003|2023-03-10 15:00:00|                  0|
# |         10018|2023-03-14 10:05:00|                  0|
# |         10017|2023-03-14 10:00:00|                  0|
# |         10004|2023-03-10 15:10:00|                  1|
# |         10019|2023-03-14 10:10:00|                  1|
# +--------------+-------------------+-------------------+

# Query 5

spark.sql("""
    SELECT
        l.category,
        COUNT(*) AS logs_cnt,
        COUNT(DISTINCT t.transaction_id) AS transaction_cnt
    FROM hw.transactions_v2 t
    JOIN hw.logs_v2 l ON t.transaction_id = l.transaction_id
    GROUP BY
        l.category
    ORDER BY
        logs_cnt DESC
""").show()

# +-----------+--------+---------------+                                          
# |   category|logs_cnt|transaction_cnt|
# +-----------+--------+---------------+
# |Electronics|       5|              4|
# |       Misc|       3|              3|
# |     Travel|       2|              2|
# |     System|       1|              1|
# |      Other|       1|              1|
# +-----------+--------+---------------+

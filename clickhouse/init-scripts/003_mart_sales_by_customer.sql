CREATE TABLE
 lab2_reports.sales_by_customer (
 customer_id UInt32,
 customer_name String,
 country String,
 total_revenue Float64,
 order_count UInt32,
 avg_check Float64
 ) ENGINE = MergeTree ()
ORDER BY

 customer_id;

-- Топ-10 клиентов по сумме покупок
CREATE VIEW lab2_reports.v_top_10_customers AS
SELECT
    customer_name,
    total_revenue
FROM  lab2_reports.sales_by_customer
ORDER BY
    total_revenue DESC
LIMIT  10;

-- Распределение по странам
CREATE VIEW lab2_reports.v_customers_by_country AS
SELECT
    country,
    count(customer_id) AS customer_count,
    sum(total_revenue) AS country_revenue
FROM  lab2_reports.sales_by_customer
GROUP BY
    country;

-- Средний чек
CREATE VIEW lab2_reports.v_customer_avg_check AS
SELECT
    customer_name,
    avg_check
FROM  lab2_reports.sales_by_customer;

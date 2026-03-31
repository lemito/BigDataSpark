CREATE TABLE lab2_reports.sales_by_store (
 store_id UInt32,
 store_name String,
 city String,
 country String,
 total_revenue Float64,
 order_count UInt32,
 avg_check Float64
 ) ENGINE = MergeTree ()
ORDER BY
    store_id;

-- Топ-5 магазинов по выручке
CREATE VIEW lab2_reports.v_top_5_stores AS
SELECT
    store_name,
    city,
    total_revenue
FROM  lab2_reports.sales_by_store
ORDER BY
    total_revenue DESC
LIMIT  5;

-- Продажи по городам и странам
CREATE VIEW lab2_reports.v_store_geography AS
SELECT
    country,
    city,
    sum(total_revenue) AS geo_revenue,
    count(store_id) AS store_count
FROM  lab2_reports.sales_by_store
GROUP BY
    country,
    city;

-- Средний чек для каждого магазина
CREATE VIEW lab2_reports.v_store_avg_check AS
SELECT
    store_name,
    city,
    country,
    avg_check,
    order_count
FROM lab2_reports.sales_by_store
ORDER BY
    avg_check DESC;

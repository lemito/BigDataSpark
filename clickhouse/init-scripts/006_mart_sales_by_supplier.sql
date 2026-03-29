CREATE TABLE lab2_reports.sales_by_supplier (
 supplier_id UInt32,
 supplier_name String,
 country String,
 total_revenue Float64,
 sales_count UInt32,
 avg_price Float64
 ) ENGINE = MergeTree ()
ORDER BY supplier_id;

-- Топ-5 поставщиков
CREATE VIEW lab2_reports.v_top_5_suppliers AS
SELECT
    supplier_name,
    total_revenue
FROM  lab2_reports.sales_by_supplier
ORDER BY
    total_revenue DESC
LIMIT  5;

-- Распределение по странам поставщиков
CREATE VIEW lab2_reports.v_suppliers_by_country AS
SELECT
    country,
    sum(total_revenue) AS supplier_revenue,
    count(supplier_id) AS supplier_count
FROM  lab2_reports.sales_by_supplier
GROUP BY
    country;

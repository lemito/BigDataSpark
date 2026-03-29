CREATE TABLE lab2_reports.sales_by_time (
 year UInt16,
 month UInt8,
 total_revenue Float64,
 order_count UInt32,
 item_count UInt32,
 avg_items_per_order Float32
 ) ENGINE = MergeTree ()
ORDER BY

 (year, month);

-- Месячные и годовые тренды
CREATE VIEW lab2_reports.v_sales_trends AS
SELECT
 year,
 month,
 total_revenue,
 order_count
FROM  lab2_reports.sales_by_time
ORDER BY
 year,
 month;

-- Средний размер заказа по месяцам
CREATE VIEW lab2_reports.v_monthly_order_size AS
SELECT
 year,
 month,
 avg_items_per_order
FROM  lab2_reports.sales_by_time;

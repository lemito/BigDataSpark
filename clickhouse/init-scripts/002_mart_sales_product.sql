CREATE TABLE
 lab2_reports.sales_by_product (
 product_id UInt32,
 product_name String,
 category String,
 total_revenue Float64,
 sales_count UInt32,
 avg_rating Float32,
 review_count UInt32
 ) ENGINE = MergeTree ()
ORDER BY

 product_id;

-- Топ-10 самых продаваемых продуктов
CREATE VIEW lab2_reports.v_top_10_products AS
SELECT
    product_name,
    sales_count,
    total_revenue
FROM  lab2_reports.sales_by_product
ORDER BY
    sales_count DESC
LIMIT  10;

-- Общая выручка по категориям
CREATE VIEW lab2_reports.v_revenue_by_category AS
SELECT
    category,
    sum(total_revenue) AS category_revenue,
    sum(sales_count) AS total_sales
FROM  lab2_reports.sales_by_product
GROUP BY
    category;

-- Рейтинги и отзывы (информационная вью)
CREATE VIEW lab2_reports.v_product_popularity AS
SELECT
    product_name,
    avg_rating,
    review_count
FROM  lab2_reports.sales_by_product;

CREATE TABLE lab2_reports.product_quality (
 product_id UInt32,
 product_name String,
 category String,
 avg_rating Float32,
 review_count UInt32,
 sales_count UInt32
 ) ENGINE = MergeTree ()
ORDER BY
    product_id;

-- Продукты с наивысшим и наименьшим рейтингом
CREATE VIEW lab2_reports.v_product_rating_extremes AS
(SELECT 'Best' AS type, product_name, avg_rating FROM lab2_reports.product_quality ORDER BY avg_rating DESC LIMIT 5)
UNION ALL
(SELECT 'Worst' AS type, product_name, avg_rating FROM lab2_reports.product_quality ORDER BY avg_rating ASC LIMIT 5);

-- Корреляция (зависимость продаж от рейтинга)
CREATE VIEW lab2_reports.v_rating_vs_sales AS
SELECT
 round(avg_rating) AS rating_group,
 avg(sales_count) AS avg_sales_volume,
 count() AS products_in_group
FROM  lab2_reports.product_quality
GROUP BY
    rating_group
ORDER BY
    rating_group;

-- Самые обсуждаемые товары
CREATE VIEW lab2_reports.v_most_reviewed_products AS
SELECT
 product_name,
 review_count
FROM  lab2_reports.product_quality
ORDER BY
    review_count DESC
LIMIT  10;

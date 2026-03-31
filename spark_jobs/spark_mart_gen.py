from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = (
    SparkSession.builder.appName("MartsToClickHouse")
    .config("spark.sql.catalog.clickhouse", "com.clickhouse.spark.ClickHouseCatalog")
    .config("spark.sql.catalog.clickhouse.host", "clickhouse")
    .getOrCreate()
)

PG_URL = "jdbc:postgresql://postgres:5432/spark_db"
PG_PROPS = {"user": "meow", "password": "UwU", "driver": "org.postgresql.Driver"}


def pg_read(table: str) -> DataFrame:
    return spark.read.jdbc(url=PG_URL, table=f"{table}", properties=PG_PROPS)


def ch_write(df: DataFrame, table: str) -> None:
    df.writeTo(f"clickhouse.lab2_reports.{table}").append()


fact = pg_read("fact_sales").cache()
dim_products = pg_read("dim_products")
dim_customers = pg_read("dim_customers")
dim_stores = pg_read("dim_stores")
dim_suppliers = pg_read("dim_suppliers")
dim_prod_cats = pg_read("dim_product_categories")
dim_countries = pg_read("dim_countries")
dim_cities = pg_read("dim_cities")


sales_by_product = (
    fact.join(dim_products.alias("p"), "product_id")
    .join(dim_prod_cats.alias("pc"), F.col("p.category_id") == F.col("pc.category_id"))
    .groupBy(
        F.col("p.product_id"),
        F.col("p.name").alias("product_name"),
        F.col("pc.name").alias("category"),
    )
    .agg(
        F.sum("total_price").cast("double").alias("total_revenue"),
        F.count("*").cast("integer").alias("sales_count"),
        F.avg("p.rating").cast("float").alias("avg_rating"),
        F.sum("p.reviews").cast("integer").alias("review_count"),
    )
)
ch_write(sales_by_product, "sales_by_product")


sales_by_customer = (
    fact.join(dim_customers.alias("cu"), "customer_id")
    .join(dim_countries.alias("cn"), F.col("cu.country_id") == F.col("cn.country_id"))
    .groupBy(
        F.col("cu.customer_id"),
        F.concat_ws(" ", F.col("cu.first_name"), F.col("cu.last_name")).alias(
            "customer_name"
        ),
        F.col("cn.name").alias("country"),
    )
    .agg(
        F.sum("total_price").cast("double").alias("total_revenue"),
        F.count("*").cast("integer").alias("order_count"),
        F.avg("total_price").cast("double").alias("avg_check"),
    )
)
ch_write(sales_by_customer, "sales_by_customer")


sales_by_time = (
    fact.withColumn("year", F.year("date"))
    .withColumn("month", F.month("date"))
    .groupBy("year", "month")
    .agg(
        F.sum("total_price").cast("double").alias("total_revenue"),
        F.count("*").cast("integer").alias("order_count"),
        F.sum("quantity").cast("integer").alias("item_count"),
        F.avg("quantity").cast("float").alias("avg_items_per_order"),
    )
)
ch_write(sales_by_time, "sales_by_time")


sales_by_store = (
    fact.join(dim_stores.alias("st"), "store_id")
    .join(dim_cities.alias("ci"), F.col("st.city_id") == F.col("ci.city_id"))
    .join(dim_countries.alias("cn"), F.col("st.country_id") == F.col("cn.country_id"))
    .groupBy(
        F.col("st.store_id"),
        F.col("st.name").alias("store_name"),
        F.col("ci.name").alias("city"),
        F.col("cn.name").alias("country"),
    )
    .agg(
        F.sum("total_price").cast("double").alias("total_revenue"),
        F.count("*").cast("integer").alias("order_count"),
        F.avg("total_price").cast("double").alias("avg_check"),
    )
)
ch_write(sales_by_store, "sales_by_store")


sales_by_supplier = (
    fact.join(dim_products.alias("p"), "product_id")
    .join(dim_suppliers.alias("su"), F.col("p.supplier_id") == F.col("su.supplier_id"))
    .join(dim_countries.alias("cn"), F.col("su.country_id") == F.col("cn.country_id"))
    .groupBy(
        F.col("su.supplier_id"),
        F.col("su.name").alias("supplier_name"),
        F.col("cn.name").alias("country"),
    )
    .agg(
        F.sum("total_price").cast("double").alias("total_revenue"),
        F.count("*").cast("integer").alias("sales_count"),
        F.avg("p.price").cast("double").alias("avg_price"),
    )
)
ch_write(sales_by_supplier, "sales_by_supplier")


product_quality = (
    dim_products.alias("p")
    .join(dim_prod_cats.alias("pc"), F.col("p.category_id") == F.col("pc.category_id"))
    .join(
        fact.groupBy("product_id").agg(
            F.count("*").cast("integer").alias("sales_count")
        ),
        "product_id",
        "left",
    )
    .select(
        F.col("p.product_id"),
        F.col("p.name").alias("product_name"),
        F.col("pc.name").alias("category"),
        F.col("p.rating").cast("float").alias("avg_rating"),
        F.col("p.reviews").cast("integer").alias("review_count"),
        F.coalesce(F.col("sales_count"), F.lit(0)).alias("sales_count"),
    )
)
ch_write(product_quality, "product_quality")

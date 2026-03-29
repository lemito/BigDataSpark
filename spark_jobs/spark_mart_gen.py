from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = (
    SparkSession.builder.appName("SnowflakeToClickHouse")
    .config("spark.sql.catalog.clickhouse", "com.clickhouse.spark.ClickHouseCatalog")
    .config("spark.sql.catalog.clickhouse.host", "clickhouse")
    .config("spark.sql.catalog.clickhouse.http_port", "8123")
    .config("spark.sql.catalog.clickhouse.user", "meow")
    .config("spark.sql.catalog.clickhouse.password", "UwU")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

PG_URL = "jdbc:postgresql://postgres:5432/spark_db"
PG_PROPS = {"user": "meow", "password": "UwU", "driver": "org.postgresql.Driver"}


def pg_read(table: str) -> DataFrame:
    print(f"Reading {table} from PostgreSQL...")
    return spark.read.jdbc(url=PG_URL, table=f"{table}", properties=PG_PROPS)


def ch_write(df: DataFrame, table: str) -> None:
    (
        df.writeTo(f"clickhouse.lab2_reports.{table}")
        .option("batch_size", "50000")
        .append()
    )
    print(f"Written {table} to clickhouse...")


print("Reading snowflake schema from PostgreSQL...")

fact = pg_read("fact_sales").cache()
dim_products = pg_read("dim_products")
dim_customers = pg_read("dim_customers")
dim_stores = pg_read("dim_stores")
dim_suppliers = pg_read("dim_suppliers")
dim_prod_cats = pg_read("dim_product_categories")
dim_countries = pg_read("dim_countries")
dim_cities = pg_read("dim_cities")
dim_states = pg_read("dim_states")

#  sales_by_product
#     Schema: product_id, product_name, category, total_revenue,
#             sales_count, avg_rating, review_count
print("Building sales_by_product...")

sales_by_product = (
    fact.join(
        dim_products.select(
            "product_id",
            F.col("name").alias("product_name"),
            "category_id",
            "rating",
            "reviews",
        ),
        "product_id",
    )
    .join(
        dim_prod_cats.select(
            "category_id",
            F.col("name").alias("category"),
        ),
        "category_id",
    )
    .groupBy("product_id", "product_name", "category")
    .agg(
        F.round(F.sum("total_price"), 2).cast("double").alias("total_revenue"),
        F.count("*").cast("int").alias("sales_count"),
        F.round(F.avg("rating"), 2).cast("float").alias("avg_rating"),
        F.sum("reviews").cast("int").alias("review_count"),
    )
)

ch_write(sales_by_product, "sales_by_product")


#  sales_by_customer
#     Schema: customer_id, customer_name, country, total_revenue,
#             order_count, avg_check
print("Building sales_by_customer...")

customer_geo = (
    dim_customers.join(dim_cities.select("city_id", "state_id"), "city_id")
    .join(dim_states.select("state_id", "country_id"), "state_id")
    .join(
        dim_countries.select("country_id", F.col("name").alias("country")), "country_id"
    )
    .select(
        "customer_id",
        F.concat_ws(" ", "first_name", "last_name").alias("customer_name"),
        "country",
    )
)

sales_by_customer = (
    fact.join(customer_geo, "customer_id")
    .groupBy("customer_id", "customer_name", "country")
    .agg(
        F.round(F.sum("total_price"), 2).cast("double").alias("total_revenue"),
        F.count("*").cast("int").alias("order_count"),
    )
    .withColumn(
        "avg_check",
        F.round(F.col("total_revenue") / F.col("order_count"), 2).cast("double"),
    )
)

ch_write(sales_by_customer, "sales_by_customer")


#  sales_by_time
#     Schema: year, month, total_revenue, order_count, item_count,
#             avg_items_per_order
print("Building sales_by_time...")
sales_by_time = (
    fact.filter(F.col("date").isNotNull())
    .withColumn("year", F.year("date").cast("int"))
    .withColumn("month", F.month("date").cast("int"))
    .groupBy("year", "month")
    .agg(
        F.round(F.sum("total_price"), 2).cast("double").alias("total_revenue"),
        F.count("*").cast("int").alias("order_count"),
        F.sum("quantity").cast("int").alias("item_count"),
    )
    .withColumn(
        "avg_items_per_order",
        F.round(F.col("item_count") / F.col("order_count"), 2).cast("float"),
    )
    .orderBy("year", "month")
)

ch_write(sales_by_time, "sales_by_time")


#  sales_by_store
#     Schema: store_id, store_name, city, country, total_revenue,
#             order_count, avg_check
print("Building sales_by_store...")
store_geo = (
    dim_stores.join(
        dim_cities.select("city_id", "state_id", F.col("name").alias("city")), "city_id"
    )
    .join(dim_states.select("state_id", "country_id"), "state_id")
    .join(
        dim_countries.select("country_id", F.col("name").alias("country")), "country_id"
    )
    .select(
        "store_id",
        F.col("name").alias("store_name"),
        "city",
        "country",
    )
)

sales_by_store = (
    fact.join(store_geo, "store_id")
    .groupBy("store_id", "store_name", "city", "country")
    .agg(
        F.round(F.sum("total_price"), 2).cast("double").alias("total_revenue"),
        F.count("*").cast("int").alias("order_count"),
    )
    .withColumn(
        "avg_check",
        F.round(F.col("total_revenue") / F.col("order_count"), 2).cast("double"),
    )
)

ch_write(sales_by_store, "sales_by_store")


#  sales_by_supplier
#     Schema: supplier_id, supplier_name, country, total_revenue,
#             sales_count, avg_price
print("Building sales_by_supplier...")
supplier_geo = (
    dim_suppliers.join(dim_cities.select("city_id", "state_id"), "city_id")
    .join(dim_states.select("state_id", "country_id"), "state_id")
    .join(
        dim_countries.select("country_id", F.col("name").alias("country")), "country_id"
    )
    .select(
        "supplier_id",
        F.col("name").alias("supplier_name"),
        "country",
    )
)

fact_with_supplier = fact.join(
    dim_products.select("product_id", "supplier_id", "price"), "product_id"
).join(supplier_geo, "supplier_id")

sales_by_supplier = fact_with_supplier.groupBy(
    "supplier_id", "supplier_name", "country"
).agg(
    F.round(F.sum("total_price"), 2).cast("double").alias("total_revenue"),
    F.count("*").cast("int").alias("sales_count"),
    F.round(F.avg("price"), 2).cast("double").alias("avg_price"),
)

ch_write(sales_by_supplier, "sales_by_supplier")


#  product_quality
#     Schema: product_id, product_name, category, avg_rating,
#             review_count, sales_count
print("Building product_quality...")

product_quality = (
    fact.join(
        dim_products.select(
            "product_id",
            F.col("name").alias("product_name"),
            "category_id",
            "rating",
            "reviews",
        ),
        "product_id",
    )
    .join(
        dim_prod_cats.select("category_id", F.col("name").alias("category")),
        "category_id",
    )
    .groupBy("product_id", "product_name", "category")
    .agg(
        F.round(F.avg("rating"), 2).cast("float").alias("avg_rating"),
        F.sum("reviews").cast("int").alias("review_count"),
        F.count("*").cast("int").alias("sales_count"),
    )
)

ch_write(product_quality, "product_quality")

print("\nUwU DONE")
spark.stop()

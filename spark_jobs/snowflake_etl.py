from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DecimalType

spark = SparkSession.builder.appName("StarSchemaETL").getOrCreate()

PG_URL = "jdbc:postgresql://postgres:5432/spark_db"
PG_PROPS = {
    "user": "meow",
    "password": "UwU",
    "driver": "org.postgresql.Driver",
}


def pg_read(table: str) -> DataFrame:
    return spark.read.jdbc(url=PG_URL, table=table, properties=PG_PROPS)


def pg_write(df: DataFrame, table: str) -> None:
    df.write.jdbc(url=PG_URL, table=table, mode="append", properties=PG_PROPS)


def parse_sql_date(col_name: str) -> F.Column:
    return F.when(
        F.col(col_name).rlike(r"^\d{1,2}/\d{1,2}/\d{4}$"),
        F.to_date(F.col(col_name), "M/d/yyyy"),
    ).otherwise(F.lit(None).cast("date"))


def get_complex_id(base_id_col: str) -> F.Column:
    return (F.col(base_id_col) + F.floor((F.col("id") - 1) / 1000) * 1000).cast(
        IntegerType()
    )


raw = pg_read("mock_data").cache()


dim_pets = (
    raw.filter(
        F.col("customer_pet_name").isNotNull() & F.col("sale_customer_id").isNotNull()
    )
    .select(
        get_complex_id("sale_customer_id").alias("pet_id"),
        F.col("customer_pet_name").alias("name"),
        F.col("customer_pet_breed").alias("breed_name"),
        F.col("customer_pet_type").alias("pet_type_name"),
    )
    .distinct()
)
pg_write(dim_pets, "dim_pets")


dim_customers = (
    raw.filter(F.col("sale_customer_id").isNotNull())
    .select(
        get_complex_id("sale_customer_id").alias("customer_id"),
        F.col("customer_first_name").alias("first_name"),
        F.col("customer_last_name").alias("last_name"),
        F.col("customer_email").alias("email"),
        F.col("customer_age").alias("age"),
        F.col("customer_country").alias("country"),
        F.col("customer_postal_code").alias("postal_code"),
        get_complex_id("sale_customer_id").alias("pet_id"),
    )
    .distinct()
)
pg_write(dim_customers, "dim_customers")


dim_sellers = (
    raw.filter(F.col("sale_seller_id").isNotNull())
    .select(
        get_complex_id("sale_seller_id").alias("seller_id"),
        F.col("seller_first_name").alias("first_name"),
        F.col("seller_last_name").alias("last_name"),
        F.col("seller_email").alias("email"),
        F.col("seller_country").alias("country"),
        F.col("seller_postal_code").alias("postal_code"),
    )
    .distinct()
)
pg_write(dim_sellers, "dim_sellers")


dim_stores = (
    raw.filter(F.col("store_email").isNotNull())
    .select(
        (F.monotonically_increasing_id() + 1).alias("store_id"),
        F.col("store_name").alias("name"),
        F.col("store_location").alias("location"),
        F.col("store_city").alias("city"),
        F.col("store_state").alias("state"),
        F.col("store_country").alias("country"),
        F.col("store_phone").alias("phone"),
        F.col("store_email").alias("email"),
    )
    .distinct()
)
pg_write(dim_stores, "dim_stores")


dim_suppliers = (
    raw.filter(F.col("supplier_email").isNotNull())
    .select(
        (F.monotonically_increasing_id() + 1).alias("supplier_id"),
        F.col("supplier_name").alias("name"),
        F.col("supplier_contact").alias("contact"),
        F.col("supplier_email").alias("email"),
        F.col("supplier_phone").alias("phone"),
        F.col("supplier_address").alias("address"),
        F.col("supplier_city").alias("city"),
        F.col("supplier_country").alias("country"),
    )
    .distinct()
)
pg_write(dim_suppliers, "dim_suppliers")


dim_products = (
    raw.filter(F.col("sale_product_id").isNotNull())
    .join(
        dim_suppliers.alias("sup"),
        F.col("supplier_email") == F.col("sup.email"),
        "left",
    )
    .select(
        get_complex_id("sale_product_id").alias("product_id"),
        F.col("product_name").alias("name"),
        F.col("pet_category"),
        F.col("product_category").alias("category"),
        F.col("product_price").cast(DecimalType(10, 2)).alias("price"),
        F.col("product_weight").cast(DecimalType(10, 2)).alias("weight"),
        F.col("product_color").alias("color"),
        F.col("product_size").alias("size"),
        F.col("product_brand").alias("brand"),
        F.col("product_material").alias("material"),
        F.col("product_description").alias("description"),
        F.col("product_rating").cast(DecimalType(3, 1)).alias("rating"),
        F.col("product_reviews").alias("reviews"),
        parse_sql_date("product_release_date").alias("release_date"),
        parse_sql_date("product_expiry_date").alias("expiry_date"),
        F.col("sup.supplier_id"),
        F.col("sup.name").alias("supplier_name"),
        F.col("sup.city").alias("supplier_city"),
        F.col("sup.country").alias("supplier_country"),
    )
    .distinct()
)
pg_write(dim_products, "dim_products")


fact_sales = raw.join(
    dim_stores.alias("s"), F.col("store_email") == F.col("s.email"), "left"
).select(
    get_complex_id("sale_customer_id").alias("customer_id"),
    get_complex_id("sale_seller_id").alias("seller_id"),
    get_complex_id("sale_product_id").alias("product_id"),
    F.col("s.store_id"),
    F.col("sale_quantity").alias("quantity"),
    F.col("sale_total_price").cast(DecimalType(10, 2)).alias("total_price"),
    parse_sql_date("sale_date").alias("date"),
)
pg_write(fact_sales, "fact_sales")

spark.stop()

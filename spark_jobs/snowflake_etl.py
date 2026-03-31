from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DecimalType

spark = SparkSession.builder.appName("SnowflakeETL_Exact_Sync").getOrCreate()

PG_URL = "jdbc:postgresql://postgres:5432/spark_db"
PG_PROPS = {
    "user": "meow",
    "password": "UwU",
    "driver": "org.postgresql.Driver",
}


def pg_read(table: str) -> DataFrame:
    return spark.read.jdbc(url=PG_URL, table=table, properties=PG_PROPS)


def pg_write(df: DataFrame, table: str) -> None:
    full_table = f"{table}"

    df.write.jdbc(url=PG_URL, table=full_table, mode="append", properties=PG_PROPS)


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


def sync_simple_dim(source_col: str, table_name: str, id_col: str) -> DataFrame:
    df = (
        raw.select(F.col(source_col).alias("name"))
        .filter(F.col("name").isNotNull())
        .distinct()
        .withColumn(id_col, F.monotonically_increasing_id() + 1)
    )
    pg_write(df, table_name)
    return df


pet_types = sync_simple_dim("customer_pet_type", "dim_pet_types", "pet_type_id")
states = sync_simple_dim("store_state", "dim_states", "state_id")
pet_cats = sync_simple_dim("pet_category", "dim_pet_categories", "pet_category_id")
prod_cats = sync_simple_dim("product_category", "dim_product_categories", "category_id")
colors = sync_simple_dim("product_color", "dim_colors", "color_id")
brands = sync_simple_dim("product_brand", "dim_brands", "brand_id")
materials = sync_simple_dim("product_material", "dim_materials", "material_id")

countries = (
    raw.select(F.col("customer_country").alias("name"))
    .union(raw.select(F.col("seller_country").alias("name")))
    .union(raw.select(F.col("store_country").alias("name")))
    .union(raw.select(F.col("supplier_country").alias("name")))
    .filter(F.col("name").isNotNull())
    .distinct()
    .withColumn("country_id", F.monotonically_increasing_id() + 1)
)
pg_write(countries, "dim_countries")

cities = (
    raw.select(F.col("store_city").alias("name"))
    .union(raw.select(F.col("supplier_city").alias("name")))
    .filter(F.col("name").isNotNull())
    .distinct()
    .withColumn("city_id", F.monotonically_increasing_id() + 1)
)
pg_write(cities, "dim_cities")


pet_breeds = (
    raw.select(
        F.col("customer_pet_type").alias("raw_pt"),
        F.col("customer_pet_breed").alias("breed_name"),
    )
    .filter(F.col("breed_name").isNotNull())
    .distinct()
    .join(pet_types.alias("pt"), F.col("raw_pt") == F.col("pt.name"), "left")
    .select(
        (F.monotonically_increasing_id() + 1).alias("pet_breed_id"),
        F.col("pt.pet_type_id"),
        F.col("breed_name").alias("name"),
    )
)
pg_write(pet_breeds, "dim_pet_breeds")

pets = (
    raw.filter(
        F.col("customer_pet_name").isNotNull() & F.col("sale_customer_id").isNotNull()
    )
    .join(
        pet_breeds.alias("pb"), F.col("customer_pet_breed") == F.col("pb.name"), "left"
    )
    .select(
        get_complex_id("sale_customer_id").alias("pet_id"),
        F.col("customer_pet_name").alias("name"),
        F.col("pb.pet_breed_id").alias("breed_id"),
    )
    .distinct()
)
pg_write(pets, "dim_pets")

suppliers = (
    raw.filter(F.col("supplier_email").isNotNull())
    .join(cities.alias("ct"), F.col("supplier_city") == F.col("ct.name"), "left")
    .join(countries.alias("cn"), F.col("supplier_country") == F.col("cn.name"), "left")
    .select(
        (F.monotonically_increasing_id() + 1).alias("supplier_id"),
        F.col("supplier_name").alias("name"),
        F.col("supplier_contact").alias("contact"),
        F.col("supplier_email").alias("email"),
        F.col("supplier_phone").alias("phone"),
        F.col("supplier_address").alias("address"),
        F.col("ct.city_id"),
        F.col("cn.country_id"),
    )
    .distinct()
)
pg_write(suppliers, "dim_suppliers")

stores = (
    raw.filter(F.col("store_email").isNotNull())
    .join(cities.alias("ct"), F.col("store_city") == F.col("ct.name"), "left")
    .join(states.alias("st"), F.col("store_state") == F.col("st.name"), "left")
    .join(countries.alias("cn"), F.col("store_country") == F.col("cn.name"), "left")
    .select(
        (F.monotonically_increasing_id() + 1).alias("store_id"),
        F.col("store_name").alias("name"),
        F.col("store_location").alias("location"),
        F.col("ct.city_id"),
        F.col("st.state_id"),
        F.col("cn.country_id"),
        F.col("store_phone").alias("phone"),
        F.col("store_email").alias("email"),
    )
    .distinct()
)
pg_write(stores, "dim_stores")

customers = (
    raw.filter(F.col("sale_customer_id").isNotNull())
    .join(countries.alias("cn"), F.col("customer_country") == F.col("cn.name"), "left")
    .select(
        get_complex_id("sale_customer_id").alias("customer_id"),
        F.col("customer_first_name").alias("first_name"),
        F.col("customer_last_name").alias("last_name"),
        F.col("customer_email").alias("email"),
        F.col("customer_age").alias("age"),
        F.col("cn.country_id"),
        F.col("customer_postal_code").alias("postal_code"),
        get_complex_id("sale_customer_id").alias("pet_id"),
    )
    .distinct()
)
pg_write(customers, "dim_customers")

sellers = (
    raw.filter(F.col("sale_seller_id").isNotNull())
    .join(countries.alias("cn"), F.col("seller_country") == F.col("cn.name"), "left")
    .select(
        get_complex_id("sale_seller_id").alias("seller_id"),
        F.col("seller_first_name").alias("first_name"),
        F.col("seller_last_name").alias("last_name"),
        F.col("seller_email").alias("email"),
        F.col("cn.country_id"),
        F.col("seller_postal_code").alias("postal_code"),
    )
    .distinct()
)
pg_write(sellers, "dim_sellers")

products = (
    raw.filter(F.col("sale_product_id").isNotNull())
    .join(pet_cats.alias("pcat"), F.col("pet_category") == F.col("pcat.name"), "left")
    .join(
        prod_cats.alias("cat"), F.col("product_category") == F.col("cat.name"), "left"
    )
    .join(colors.alias("col"), F.col("product_color") == F.col("col.name"), "left")
    .join(brands.alias("br"), F.col("product_brand") == F.col("br.name"), "left")
    .join(
        materials.alias("mat"), F.col("product_material") == F.col("mat.name"), "left"
    )
    .join(suppliers.alias("sup"), F.col("supplier_email") == F.col("sup.email"), "left")
    .select(
        get_complex_id("sale_product_id").alias("product_id"),
        F.col("product_name").alias("name"),
        F.col("pcat.pet_category_id"),
        F.col("cat.category_id"),
        F.col("product_price").cast(DecimalType(10, 2)).alias("price"),
        F.col("product_weight").cast(DecimalType(10, 2)).alias("weight"),
        F.col("col.color_id"),
        F.col("product_size").alias("size"),
        F.col("br.brand_id"),
        F.col("mat.material_id"),
        F.col("product_description").alias("description"),
        F.col("product_rating").cast(DecimalType(3, 1)).alias("rating"),
        F.col("product_reviews").alias("reviews"),
        parse_sql_date("product_release_date").alias("release_date"),
        parse_sql_date("product_expiry_date").alias("expiry_date"),
        F.col("sup.supplier_id"),
    )
    .distinct()
)
pg_write(products, "dim_products")

fact_sales = raw.join(
    stores.alias("s"), F.col("store_email") == F.col("s.email"), "left"
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

"""
Создание снежинки и её наполнение из mock-data
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType

spark = SparkSession.builder.appName("SnowflakeETL").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

PG_URL = "jdbc:postgresql://postgres:5432/spark_db"
PG_PROPS = {"user": "meow", "password": "UwU", "driver": "org.postgresql.Driver"}


def pg_read(table: str) -> DataFrame:
    return spark.read.jdbc(url=PG_URL, table=table, properties=PG_PROPS)


def pg_write(df: DataFrame, table: str) -> None:
    df.write.jdbc(
        url=PG_URL,
        table=table,
        mode="overwrite",
        properties={**PG_PROPS, "batchsize": "10000", "truncate": "true"},
    )
    print(f"Written {table} to PostgreSQL.")


def make_dim(df: DataFrame, id_col: str) -> DataFrame:
    rows = df.collect()
    schema = StructType(
        [StructField(id_col, IntegerType(), False)] + list(df.schema.fields)
    )
    data = [(i + 1, *row) for i, row in enumerate(rows)]
    return spark.createDataFrame(data, schema)


def parse_date(col_name: str) -> F.Column:
    return F.when(
        F.col(col_name).rlike(r"^\d{1,2}/\d{1,2}/\d{4}$"),
        F.to_date(F.col(col_name), "M/d/yyyy"),
    ).otherwise(F.lit(None).cast("date"))


def simple_dim(source_col: str, id_col: str, table: str) -> DataFrame:
    raw_df = (
        raw.select(F.col(source_col).alias("name"))
        .filter(F.col("name").isNotNull())
        .distinct()
    )
    df = make_dim(raw_df, id_col)
    pg_write(df.select(id_col, "name"), table)
    return df


print("Reading mock_data...")
raw = pg_read("mock_data").cache()
raw.count()
print("done.")


# dim_pet_types
print("dim_pet_types")
pet_types = simple_dim("customer_pet_type", "pet_type_id", "dim_pet_types")


# dim_pet_breeds
print("dim_pet_breeds")
pet_breeds_raw = (
    raw.select(
        F.col("customer_pet_type").alias("type_name"),
        F.col("customer_pet_breed").alias("name"),
    )
    .filter(F.col("name").isNotNull())
    .distinct()
    .join(
        pet_types.select("pet_type_id", F.col("name").alias("type_name")), "type_name"
    )
    .select("pet_type_id", "name")
)
pet_breeds = make_dim(pet_breeds_raw, "pet_breed_id")
pg_write(pet_breeds.select("pet_breed_id", "pet_type_id", "name"), "dim_pet_breeds")


# dim_pets
print("dim_pets")
pets = (
    raw.filter(F.col("sale_customer_id").isNotNull())
    .select(
        (F.col("sale_customer_id") + F.floor((F.col("id") - 1) / 1000) * 1000)
        .cast(IntegerType())
        .alias("pet_id"),
        F.col("customer_pet_type").alias("type_name"),
        F.col("customer_pet_breed").alias("breed_name"),
        F.col("customer_pet_name").alias("name"),
    )
    .distinct()
    .join(
        pet_types.select("pet_type_id", F.col("name").alias("type_name")), "type_name"
    )
    .join(
        pet_breeds.select(
            "pet_breed_id", "pet_type_id", F.col("name").alias("breed_name")
        ),
        on=["breed_name", "pet_type_id"],
    )
    .select("pet_id", F.col("pet_breed_id").alias("breed_id"), "name")
)
pg_write(pets, "dim_pets")


# dim_countries
print("dim_countries")
countries_raw = (
    raw.select(F.col("customer_country").alias("name"))
    .union(raw.select(F.col("store_country")))
    .union(raw.select(F.col("supplier_country")))
    .filter(F.col("name").isNotNull())
    .distinct()
)
countries = make_dim(countries_raw, "country_id")
pg_write(countries.select("country_id", "name"), "dim_countries")


# dim_states
print("dim_states")
store_states_raw = (
    raw.select(
        F.col("store_state").alias("name"),
        F.col("store_country").alias("country_name"),
    )
    .filter(F.col("name").isNotNull())
    .distinct()
    .join(
        countries.select("country_id", F.col("name").alias("country_name")),
        "country_name",
    )
    .select("name", "country_id")  # (name, country_id)
)

na_states_raw = countries.select(
    F.lit("N/A").alias("name"),  # (name, country_id)
    F.col("country_id"),
)

states = make_dim(store_states_raw.union(na_states_raw).distinct(), "state_id")
pg_write(states.select("state_id", "country_id", "name"), "dim_states")

na_state_ids = states.filter(F.col("name") == "N/A").select("state_id", "country_id")


# dim_cities
print("dim_cities")
store_cities_raw = (
    raw.select(
        F.col("store_city").alias("name"),
        F.col("store_state").alias("state_name"),
        F.col("store_country").alias("country_name"),
    )
    .filter(F.col("name").isNotNull())
    .distinct()
    .join(
        countries.select("country_id", F.col("name").alias("country_name")),
        "country_name",
    )
    .join(
        states.select("state_id", "country_id", F.col("name").alias("state_name")),
        on=["state_name", "country_id"],
    )
    .select("name", "state_id")  # (name, state_id)
)

supplier_cities_raw = (
    raw.select(
        F.col("supplier_city").alias("name"),
        F.col("supplier_country").alias("country_name"),
    )
    .filter(F.col("name").isNotNull())
    .distinct()
    .join(
        countries.select("country_id", F.col("name").alias("country_name")),
        "country_name",
    )
    .join(na_state_ids, "country_id")
    .select("name", "state_id")  # (name, state_id)
)

unknown_cities_raw = states.select(
    F.lit("Unknown City").alias("name"),  # (name, state_id)
    F.col("state_id"),
)

cities = make_dim(
    store_cities_raw.union(supplier_cities_raw).union(unknown_cities_raw).distinct(),
    "city_id",
)
pg_write(cities.select("city_id", "state_id", "name"), "dim_cities")

unknown_city_ids = cities.filter(F.col("name") == "Unknown City").select(
    "city_id", "state_id"
)


# dim_suppliers
print("dim_suppliers")
supplier_city_lookup = (
    raw.select(
        F.col("supplier_city").alias("city_name"),
        F.col("supplier_country").alias("country_name"),
    )
    .distinct()
    .join(
        countries.select("country_id", F.col("name").alias("country_name")),
        "country_name",
    )
    .join(na_state_ids, "country_id")
    .join(
        cities.select("city_id", "state_id", F.col("name").alias("city_name")),
        on=["city_name", "state_id"],
    )
    .select("city_name", "country_name", "city_id")
)

suppliers_raw = (
    raw.select(
        F.col("supplier_name").alias("name"),
        F.col("supplier_contact").alias("contact"),
        F.col("supplier_email").alias("email"),
        F.col("supplier_phone").alias("phone"),
        F.col("supplier_address").alias("address"),
        F.col("supplier_city").alias("city_name"),
        F.col("supplier_country").alias("country_name"),
    )
    .filter(F.col("name").isNotNull())
    .distinct()
    .join(supplier_city_lookup, on=["city_name", "country_name"])
    .select("name", "contact", "email", "phone", "address", "city_id")
)
suppliers = make_dim(suppliers_raw, "supplier_id")
pg_write(
    suppliers.select(
        "supplier_id", "name", "contact", "email", "phone", "address", "city_id"
    ),
    "dim_suppliers",
)


# lookup dimensions
print("lookup dims")
colors = simple_dim("product_color", "color_id", "dim_colors")
brands = simple_dim("product_brand", "brand_id", "dim_brands")
materials = simple_dim("product_material", "material_id", "dim_materials")
prod_cats = simple_dim("product_category", "category_id", "dim_product_categories")
pet_cats = simple_dim("pet_category", "pet_category_id", "dim_pet_categories")


# dim_stores
print("dim_stores")
store_city_lookup = (
    raw.select(
        F.col("store_city").alias("city_name"),
        F.col("store_state").alias("state_name"),
        F.col("store_country").alias("country_name"),
    )
    .distinct()
    .join(
        countries.select("country_id", F.col("name").alias("country_name")),
        "country_name",
    )
    .join(
        states.select("state_id", "country_id", F.col("name").alias("state_name")),
        on=["state_name", "country_id"],
    )
    .join(
        cities.select("city_id", "state_id", F.col("name").alias("city_name")),
        on=["city_name", "state_id"],
    )
    .select("city_name", "state_name", "country_name", "city_id")
)

stores_raw = (
    raw.select(
        F.col("store_name").alias("name"),
        F.col("store_location").alias("location"),
        F.col("store_phone").alias("phone"),
        F.col("store_email").alias("email"),
        F.col("store_city").alias("city_name"),
        F.col("store_state").alias("state_name"),
        F.col("store_country").alias("country_name"),
    )
    .filter(F.col("name").isNotNull())
    .distinct()
    .join(store_city_lookup, on=["city_name", "state_name", "country_name"])
    .select("name", "location", "city_id", "phone", "email")
)
stores = make_dim(stores_raw, "store_id")
pg_write(
    stores.select("store_id", "name", "location", "city_id", "phone", "email"),
    "dim_stores",
)


# dim_customers
print("dim_customers")
cust_city_lookup = (
    countries.select("country_id", F.col("name").alias("country_name"))
    .join(na_state_ids, "country_id")
    .join(unknown_city_ids, "state_id")
    .select("country_name", "city_id")
)

customers = (
    raw.filter(F.col("sale_customer_id").isNotNull())
    .select(
        (F.col("sale_customer_id") + F.floor((F.col("id") - 1) / 1000) * 1000)
        .cast(IntegerType())
        .alias("customer_id"),
        F.col("customer_first_name").alias("first_name"),
        F.col("customer_last_name").alias("last_name"),
        F.col("customer_email").alias("email"),
        F.col("customer_age").cast(IntegerType()).alias("age"),
        F.col("customer_postal_code").alias("postal_code"),
        F.col("customer_country").alias("country_name"),
        (F.col("sale_customer_id") + F.floor((F.col("id") - 1) / 1000) * 1000)
        .cast(IntegerType())
        .alias("pet_id"),
    )
    .distinct()
    .join(cust_city_lookup, "country_name")
    .select(
        "customer_id",
        "first_name",
        "last_name",
        "email",
        "age",
        "city_id",
        "postal_code",
        "pet_id",
    )
)
pg_write(customers, "dim_customers")


# dim_sellers
print("dim_sellers")
first_unknown_city_id = int(unknown_city_ids.limit(1).collect()[0]["city_id"])

sellers = (
    raw.filter(F.col("sale_seller_id").isNotNull())
    .select(
        (F.col("sale_seller_id") + F.floor((F.col("id") - 1) / 1000) * 1000)
        .cast(IntegerType())
        .alias("seller_id"),
        F.col("seller_first_name").alias("first_name"),
        F.col("seller_last_name").alias("last_name"),
        F.col("seller_email").alias("email"),
        F.col("seller_postal_code").alias("postal_code"),
    )
    .distinct()
    .withColumn("city_id", F.lit(first_unknown_city_id).cast(IntegerType()))
)
pg_write(
    sellers.select(
        "seller_id", "first_name", "last_name", "email", "city_id", "postal_code"
    ),
    "dim_sellers",
)


# dim_products
print("dim_products")
supplier_email_lookup = suppliers.select(
    "supplier_id", F.col("email").alias("supplier_email")
)

products = (
    raw.filter(F.col("sale_product_id").isNotNull())
    .select(
        (F.col("sale_product_id") + F.floor((F.col("id") - 1) / 1000) * 1000)
        .cast(IntegerType())
        .alias("product_id"),
        F.col("product_name").alias("name"),
        F.col("product_price").alias("price"),
        F.col("product_weight").alias("weight"),
        F.col("product_color").alias("color_name"),
        F.col("product_size").alias("size"),
        F.col("product_brand").alias("brand_name"),
        F.col("product_material").alias("material_name"),
        F.col("product_category").alias("category_name"),
        F.col("pet_category").alias("pet_category_name"),
        F.col("product_description").alias("description"),
        F.col("product_rating").alias("rating"),
        F.col("product_reviews").cast(IntegerType()).alias("reviews"),
        parse_date("product_release_date").alias("release_date"),
        parse_date("product_expiry_date").alias("expiry_date"),
        F.col("supplier_email"),
    )
    .distinct()
    .join(
        colors.select("color_id", F.col("name").alias("color_name")),
        "color_name",
        "left",
    )
    .join(
        brands.select("brand_id", F.col("name").alias("brand_name")),
        "brand_name",
        "left",
    )
    .join(
        materials.select("material_id", F.col("name").alias("material_name")),
        "material_name",
        "left",
    )
    .join(
        prod_cats.select("category_id", F.col("name").alias("category_name")),
        "category_name",
        "left",
    )
    .join(
        pet_cats.select("pet_category_id", F.col("name").alias("pet_category_name")),
        "pet_category_name",
        "left",
    )
    .join(supplier_email_lookup, "supplier_email", "left")
    .select(
        "product_id",
        "name",
        "pet_category_id",
        "category_id",
        "price",
        "weight",
        "color_id",
        "size",
        "brand_id",
        "material_id",
        "description",
        "rating",
        "reviews",
        "release_date",
        "expiry_date",
        "supplier_id",
    )
)
pg_write(products, "dim_products")


# fact_sales
print("fact_sales")
store_email_lookup = stores.select("store_id", F.col("email").alias("store_email"))

fact_sales = (
    raw.select(
        (F.col("sale_customer_id") + F.floor((F.col("id") - 1) / 1000) * 1000)
        .cast(IntegerType())
        .alias("customer_id"),
        (F.col("sale_seller_id") + F.floor((F.col("id") - 1) / 1000) * 1000)
        .cast(IntegerType())
        .alias("seller_id"),
        (F.col("sale_product_id") + F.floor((F.col("id") - 1) / 1000) * 1000)
        .cast(IntegerType())
        .alias("product_id"),
        F.col("store_email"),
        F.col("sale_quantity").cast(IntegerType()).alias("quantity"),
        F.col("sale_total_price").alias("total_price"),
        parse_date("sale_date").alias("date"),
    )
    .join(store_email_lookup, "store_email")
    .select(
        "customer_id",
        "seller_id",
        "product_id",
        "store_id",
        "quantity",
        "total_price",
        "date",
    )
    .dropDuplicates(["customer_id", "product_id", "date"])
)
pg_write(fact_sales, "fact_sales")

print("\nDONE!!! =UwU=")
spark.stop()

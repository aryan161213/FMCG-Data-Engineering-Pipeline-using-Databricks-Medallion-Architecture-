# Databricks notebook source
# MAGIC %md
# MAGIC **Import Required Libraries**

# COMMAND ----------

from pyspark.sql import functions as f
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC **Load Project Utilities & Initialize Notebook Widgets**

# COMMAND ----------

# MAGIC %run /Workspace/Users/aryancom16@gmail.com/consolidated_pipeline/1_setup/utilities

# COMMAND ----------

print(bronze_schema, silver_schema, gold_schema)

# COMMAND ----------

dbutils.widgets.text("catalog", "fmcg", "Catalog")
dbutils.widgets.text("data_source", "products", "Data Source")

catalog = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")

base_path = f's3://sportsbarr-dmss/{data_source}/*.csv'
print(base_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze

# COMMAND ----------

df = (
    spark.read.format("csv")
        .option("header", True)
        .option("inferSchema", True)
        .load(base_path)
        .withColumn("read_timestamp", f.current_timestamp())
        .select("*", "_metadata.file_name", "_metadata.file_size")
)

# COMMAND ----------

# print check data type
df.printSchema()

# COMMAND ----------

display(df.limit(10))

# COMMAND ----------

df.write\
 .format("delta") \
 .option("delta.enableChangeDataFeed", "true") \
 .mode("overwrite") \
 .saveAsTable(f"{catalog}.{bronze_schema}.{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver
# MAGIC

# COMMAND ----------

df_bronze = spark.sql(f"SELECT * FROM {catalog}.{bronze_schema}.{data_source};")
df_bronze.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC **Transformations**

# COMMAND ----------

# MAGIC %md
# MAGIC - 1: Drop Duplicates

# COMMAND ----------

print('Rows before duplicates dropped: ', df_bronze.count())
df_silver = df_bronze.dropDuplicates(['product_id'])
print('Rows after duplicates dropped: ', df_silver.count())

# COMMAND ----------

# MAGIC %md
# MAGIC - 2: Title case fix
# MAGIC
# MAGIC (energy bars ---> Energy Bars, protien bars ---> Protien Bars etc)

# COMMAND ----------

df_silver.select('category').distinct().show()

# COMMAND ----------

# Title case fix
df_silver = df_silver.withColumn(
    "category",
    f.when(f.col("category").isNull(), None)
     .otherwise(f.initcap("category"))
)

# COMMAND ----------

df_silver.select('category').distinct().show()

# COMMAND ----------

# MAGIC %md
# MAGIC - 3: Fix Spelling Mistake for `Protien`

# COMMAND ----------

# Replace 'protien' → 'protein' in both product_name and category


df_silver = (
    df_silver
    .withColumn(
        "category",
        f.regexp_replace(f.col("category"),"(?i)Protien","Protein")
    )
    .withColumn(
        "product_name",
        f.regexp_replace(f.col("product_name"),"(?i)Protien","Protein")
    )


)

# COMMAND ----------

display(df_silver.limit(5))

# COMMAND ----------

df_silver.select("category").distinct().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Standardizing Customer Attributes to Match Parent Company Data Model

# COMMAND ----------

#so lets do mapping now.

df_silver = (
    df_silver
    .withColumn(
        "division",
        f.when(f.col("category") == "Energy Bars",         "Nutrition Bars")
        .when(f.col("category") == "Protein Bars",         "Nutrition Bars")
        .when(f.col("category") == "Granola & Cereals",    "Breakfast Foods")
        .when(f.col("category") == "Recovery Dairy",       "Dairy & Recovery")
        .when(f.col("category") == "Healthy Snacks",       "Healthy Snacks")
        .when(f.col("category") == "Electrolyte Mix",      "Hydration & Electrolytes")
        .otherwise("Other")
    )
)

#now lets do variant eg SportsBar Energy Bar Choco Fudge(60g) becomes SportsBar Energy Bar Choco Fudge and then 60 g diff

df_silver = (
    df_silver 
    .withColumn(
        "variant",
        f.regexp_extract(f.col("product_name"), r"\((.*?)\)", 1)

    )

)

#creating new product id or surrogate key because the old product id has some invalid entries. 

df_silver = (
    df_silver
    .withColumn(
        "product_code",
        f.sha2(f.col("product_name").cast("string"),256)

    )


#now the problem is some of the product ids are invalid and we are told to replace them with 999999.

    .withColumn(
        "product_id",
        f.when(
            f.col("product_id").cast("string").rlike("^[0-9]+$"),
            f.col("product_id").cast("string")
        ).otherwise(f.lit(999999).cast("string"))
    )

#now lets rename product_name to product
    .withColumnRenamed("product_name","product")
)


# COMMAND ----------

display(df_silver)

# COMMAND ----------

df_silver = df_silver.select("product_code", "division", "category", "product", "variant", "product_id", "read_timestamp", "file_name", "file_size")

# COMMAND ----------

print(display(df_silver))

# COMMAND ----------

#now writing it to silver table

df_silver.write\
    .format("delta")\
    .option("delta.enableChangeDataFeed","true")\
    .option("mergeSchema","true")\
    .mode("overwrite")\
    .saveAsTable(f"{catalog}.{silver_schema}.{data_source}")



    


# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold

# COMMAND ----------

#here we are selecting the contents of table we need to write to gold table

df_silver = spark.sql(f"select * from {catalog}.{silver_schema}.{data_source};")
df_gold = df_silver.select("product_code", "product_id", "division", "category", "product", "variant")
df_gold.show(10)

# COMMAND ----------

#now writing to gold table

df_gold.write\
    .format("delta")\
    .option("delta.enableChangeDataFeed","true")\
    .mode("overwrite")\
    .saveAsTable(f"{catalog}.{gold_schema}.sb_dim_{data_source}")





# COMMAND ----------

# MAGIC %md
# MAGIC ## Merging Data source with parent

# COMMAND ----------

delta_table = DeltaTable.forName(spark, "fmcg.gold.dim_products")
df_child_products = spark.sql(f"SELECT product_code, division, category,product, variant FROM fmcg.gold.sb_dim_products;")
df_child_products.show(5)

# COMMAND ----------

#simply merging data of child and parent togehter. here everything gets added as they both have diff product_code.


delta_table.alias("target").merge(
    source = df_child_products.alias("source"),
    condition = "target.product_code = source.product_code"
).whenMatchedUpdate(
    set = {
        "division": "source.division",
        "category": "source.category",
        "product": "source.product",
        "variant": "source.variant"
    }
).whenNotMatchedInsert(
    values={
        "product_code": "source.product_code",
        "division": "source.division",
        "category": "source.category",
        "product": "source.product",
        "variant": "source.variant"
    }

).execute()

# Databricks notebook source
# MAGIC %md
# MAGIC spark allows
# MAGIC
# MAGIC df = spark.table("fmcg.bronze.customers")
# MAGIC spark.sql("SELECT * from fmcg.bronze.customers")
# MAGIC BOTH DO SAME THING

# COMMAND ----------

from pyspark.sql import functions as f
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %run /Workspace/Users/aryancom16@gmail.com/consolidated_pipeline/1_setup/utilities

# COMMAND ----------

print(bronze_schema,silver_schema,gold_schema)

# COMMAND ----------

dbutils.widgets.text("catalog","fmcg","Catalog")
dbutils.widgets.text("data_source", "customers","Data Source")



# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")

base_path = f's3://sportsbarr-dmss//{data_source}/*.csv'
print(base_path)

# COMMAND ----------

df = spark.read.format("csv").load(base_path)
display(df.limit(10))
#the problem is headers not there

# COMMAND ----------

df = (
    spark.read.format("csv")
    .option("header", True)
    .option("inferSchema", True)
    .load(base_path)
    .withColumn("read_timestamp", f.current_timestamp())
    .select("*","_metadata.file_name","_metadata.file_size")
)
display(df.limit(10))


# COMMAND ----------

df.printSchema()

# COMMAND ----------

#cdf will allow to track changes at row level, fmcg.bronze.data_source
df.write\
    .format("delta")\
    .option("delta.enableChangeDataFeed","true")\
    .mode("overwrite")\
    .saveAsTable(f"{catalog}.{bronze_schema}.{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### SILVER PROCESSING
# MAGIC

# COMMAND ----------

# we will work on bronze data
#creating a new dataframe which include the bronze data
df_bronze = spark.sql(f"SELECT * from {catalog}.{bronze_schema}.{data_source};") 
df_bronze.show(10)

# COMMAND ----------

df_bronze.printSchema()

# COMMAND ----------

df_bronze

# COMMAND ----------

df_duplicates = df_bronze.groupBy("customer_id").count().where("count > 1")
#can also be done by
# df_duplicates = df_bronze.groupBy("customer_id").count().filter(f.col("count") > 1)
display(df_duplicates)

# COMMAND ----------

print("Rows before dropping duplicates : ", df_bronze.count())
df_silver = df_bronze.dropDuplicates()
print("Rows after dropping duplicates : ", df_silver.count())

# COMMAND ----------

display(
    df_silver.filter(f.col("customer_name")!= f.trim(f.col("customer_name")))
)
#returns customer name haivng leading spaces

# COMMAND ----------

df_silver = df_silver.withColumn("customer_name",f.trim(f.col("customer_name")))

# COMMAND ----------

display(df_silver)

# COMMAND ----------

df_silver.select("city").distinct().show()

# COMMAND ----------

#lets remove the spelling mistakes, wrong alphabets ,etc
city_mapping = {
    'Bengalore' : 'Bengaluru',
    'Bengaluruu' : 'Bengaluru',

    'Hyderabadd' : 'Hyderabad',
    'Hyderbad' : 'Hyderabad',

    'NewDelhi' : 'New Delhi',
    'NewDheli' : 'New Delhi',
    'NewDelhee' : 'New Delhi',

}

allowed = ['Bengaluru','Hyderabad','New Delhi']

df_silver = (
    df_silver
    .replace(city_mapping, subset=["city"])
    .withColumn(
        "city",
        f.when(f.col("city").isNull(), None)
         .when(f.col("city").isin(allowed), f.col("city"))
         .otherwise(None)
    )
)


# COMMAND ----------

df_silver.select("city").distinct().show()

# COMMAND ----------

df_silver

# COMMAND ----------

df_silver.select("customer_name").distinct().show()

# COMMAND ----------

df_silver = df_silver.withColumn(
    "customer_name", 
    f.when(f.col("customer_name").isNull() ,None)
    .otherwise(f.initcap("customer_name"))
)
df_silver.select("customer_name").distinct().show()

# COMMAND ----------

df_silver.show(10)

# COMMAND ----------

df_silver.filter(f.col("city").isNull()).show(truncate = False)



# COMMAND ----------

#i want to see any of the records which have any of these four customers
null_customer_names = ['Sprintx Nutrition','Zenathlete Foods','Primefuel Nutrition','Recovery Lane']
df_silver.filter(f.col("customer_name").isin(null_customer_names)).show()

# COMMAND ----------

# We will ask business manager for clarifications.
customer_city_fix = {
    #for Sprintx Nutrition
    789403 : "New Delhi",

    #for Zenathlete 
    789420 : "Bengaluru",

    #for Primefuel Nutrition
    789521 : "Hyderabad",

    #for Recovery Lane

    789603 : "Hyderabad"
}

df_fix = spark.createDataFrame(
    [(k,v) for k,v in customer_city_fix.items()]
,['customer_id','fixed_city'])

display(df_fix)

# COMMAND ----------

df_silver = (
    df_silver
    .join(df_fix,"customer_id","left")
    .withColumn(
        "city",
        f.coalesce("city","fixed_city")
    )
    .drop("fixed_city")

)
display(df_silver)
#so the null value in city are replaced.

# COMMAND ----------

# lastly customer_id column should be string its currently integer.
df_silver.printSchema()
df_silver = df_silver.withColumn("customer_id",f.col("customer_id").cast("string"))
df_silver.printSchema()

# COMMAND ----------

df_silver = (
    df_silver
    #building customer column with customer_name and city combined using concat(f.concat_ws).
    .withColumn(
        "customer",
        f.concat_ws("-","customer_name",f.coalesce(f.col("city"),f.lit("unknown")))

    )
    #adding three new columns
    .withColumn("market",f.lit("India"))
    .withColumn("platform",f.lit("Sports Bar"))
    .withColumn("channel",f.lit("Acquisition"))

)
display(df_silver.limit(5))




# COMMAND ----------

#lets write after this
df_silver.write\
.format("delta")\
.option("mergeSchema", "true")\
.option("delta.enableChangeDataFeed","true")\
.mode("overwrite")\
.saveAsTable(f"{catalog}.{silver_schema}.{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Gold processing
# MAGIC

# COMMAND ----------

df_silver = spark.sql(f"select * from {catalog}.{silver_schema}.{data_source};")
df_gold = df_silver.select("customer_id","customer_name","city","customer","market","platform","channel")

display(df_gold)

# COMMAND ----------

df_gold.write\
    .format("delta")\
    .option("delta.enableChangeDataFeed","true")\
    .mode("overwrite")\
    .saveAsTable(f"{catalog}.{gold_schema}.sb_dim_{data_source}")

# COMMAND ----------

delta_table = DeltaTable.forName(
    spark,"fmcg.gold.dim_customers"
)
df_child_customers = spark.table(
    "fmcg.gold.sb_dim_customers").select(f.col("customer_id").alias("customer_code"),
    "customer",
    "market",
    "platform",
    "channel"

)

# COMMAND ----------

#merge performs upsert without creating duplicates
# we need a source(child) and target(parent) and condition

delta_table.alias("target").merge(
    source = df_child_customers.alias("source"),
    condition ="target.customer_code = source.customer_code"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()



# COMMAND ----------


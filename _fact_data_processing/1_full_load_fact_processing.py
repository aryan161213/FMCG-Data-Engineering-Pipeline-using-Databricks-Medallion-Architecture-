# Databricks notebook source
from pyspark.sql import functions as f
from delta.tables import DeltaTable
from pyspark.sql.window import Window


# COMMAND ----------

# MAGIC %run /Workspace/Users/aryancom16@gmail.com/consolidated_pipeline/1_setup/utilities

# COMMAND ----------

print(bronze_schema,silver_schema,gold_schema)

# COMMAND ----------

dbutils.widgets.text("catalog","fmcg","Catalog")
dbutils.widgets.text("data_source","orders","Data Source")

catalog = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")

base_path = f's3://sportsbarr-dmss/{data_source}'
landing_path = f"{base_path}/landing/"
processed_path = f"{base_path}/processed/"

print("Base Path: " ,base_path)
print("Landing Path: " ,landing_path)
print("Processed Path: ", processed_path)


#Define Tables
bronze_table = f"{catalog}.{bronze_schema}.{data_source}"
silver_table = f"{catalog}.{silver_schema}.{data_source}"
gold_table = f"{catalog}.{gold_schema}.sb_fact_{data_source}"


# COMMAND ----------

df = spark.read.options(header = True, inferSchema = True).csv(f"{landing_path}/*.csv").withColumn("read_timestamp",f.current_timestamp()).select("*","_metadata.file_name","_metadata.file_size")

print("Total no of rows:", df.count())
df.show(10)

# COMMAND ----------

display(df.limit(10))

# COMMAND ----------

#now writing to bronze table

df.write\
    .format("delta")\
    .option("delta.enableChangeDataFeed", "true")\
    .mode("append")\
    .saveAsTable(bronze_table) 


# COMMAND ----------

files = dbutils.fs.ls(landing_path)
files

# COMMAND ----------

#lets see all the files in landing path .fs.ls

files = dbutils.fs.ls(landing_path)
for file_info in files:
    dbutils.fs.mv(
        #from 
        file_info.path,
        #to
        f"{processed_path}/{file_info.name}",
        True
    )

# COMMAND ----------

df_orders = spark.sql(f"select * from {bronze_table}")
df_orders.show(10)

# COMMAND ----------

#Orders 
df_orders = df_orders.filter(f.col("order_qty").isNotNull())

#for customer_id we need to kep it numeric or set to 999999.
#matlab jabhi customer_id number hai toh vohi return karenge ya phir 999999 return karenge

df_orders = df_orders.withColumn(
    "customer_id",
    f.when(f.col("customer_id").rlike("^[0-9]+$"),f.col("customer_id"))
     .otherwise("999999")#automatically made a string as it is inside double quotes


)
#here we remove weekday name from date text
df_orders = df_orders.withColumn(
    "order_placement_date",
    f.regexp_replace(f.col("order_placement_date"),r"^[A-Za-z]+,\s*","")                   
)


#parsing the dates in order_placement_date column in uniform format

df_orders =df_orders.withColumn(
    "order_placement_date",
    f.coalesce(
        f.try_to_date("order_placement_date","yyyy/MM/dd"),
        f.try_to_date("order_placement_date","dd-MM-yyyy"),
        f.try_to_date("order_placement_date","dd/MM/yyyy"),
        f.try_to_date("order_placement_date","MMMM dd, yyyy")
    )

)

#dropping duplicates

df_orders = df_orders.dropDuplicates(["order_id","order_placement_date","customer_id","product_id","order_qty"])

#converting product_id to string

df_orders = df_orders.withColumn("product_id",f.col("product_id").cast("string"))


# COMMAND ----------

#now lets check what is min and max date using agg
df_orders.agg(
        f.min("order_placement_date").alias("min_date"),
        f.max("order_placement_date").alias("max_date")
    ).show()


# COMMAND ----------

display(df_orders.select("customer_id"))

# COMMAND ----------

df_products = spark.table("fmcg.silver.products")
display(df_products.limit(10))

# COMMAND ----------

#Lets get product code from products table in our orders table acc to each product id using join

df_joined = df_orders.join(df_products,on= "product_id",how="inner").select(df_orders["*"],df_products["product_code"])
display(df_joined.limit(10))



# COMMAND ----------

#now we will write to our silver table


if not spark.catalog.tableExists(silver_table):
    (
        df_joined.write
        .format("delta")
        .option("delta.enableChangeDataFeed","true")
        .option("mergeSchema","true")
        .mode("overwrite")
        .saveAsTable(silver_table)
    )
else:
    #we do upsert/merge here, so for that we need delta table object
    silver_delta = DeltaTable.forName(spark,silver_table)
    (
        silver_delta.alias("silver").merge(
        
            df_joined.alias("bronze"),"silver.order_placement_date = bronze.order_placement_date AND silver.order_id = bronze.order_id AND silver.product_code = bronze.product_code AND silver.customer_id = bronze.customer_id" 

        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

    )


# COMMAND ----------

# MAGIC %md
# MAGIC #####Gold

# COMMAND ----------

#now lets read and name it gold table 
df_gold = spark.sql(f"select order_id , order_placement_date as date, customer_id as customer_code,product_code,product_id,order_qty as sold_quantity from {silver_table};")

df_gold.show(5)

# COMMAND ----------

gold_table

# COMMAND ----------

df_gold

# COMMAND ----------

#now we do write for gold table, we havent created the final gold table but saving it as gold table now only.
#now we are preparing our actual df_gold.
#here if not tableexists we create table and if exists we do upsert

if not spark.catalog.tableExists(gold_table): #spark mai catalog mai table exist nahi kartay toh create karo
    print("creating new Table")
    (df_gold.write
        .format("delta")
        .option("delta.enableChangeDataFeed","true")
        .option("mergeSchema","true")
        .mode("overwrite")
        .saveAsTable(gold_table)
    )
else:
    gold_delta = DeltaTable.forName(spark,gold_table)
    (
    gold_delta.alias("source").merge(
            df_gold.alias("gold"),"source.date = gold.date AND source.order_id = gold.order_id AND source.product_code = gold.product_code AND source.customer_code = gold.customer_code"
        
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    )
    


# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ### Merge with PARENT
# MAGIC

# COMMAND ----------

#FIRST STEP LOADING THE CHILD DATA WHICH WE WORKED ON JUST NOW

df_child = spark.sql(f"SELECT date, product_code, customer_code, sold_quantity from {gold_table}")
df_child.show(10)



# COMMAND ----------

df_child.count()

# COMMAND ----------

#lets first change the date to show the first date of month like 
# 2025-07-12 => 2025-07-1

df_monthly = (
    df_child.withColumn(
        "month_start",
        f.trunc("date","MM")

    )
    .groupBy("month_start","product_code","customer_code")
    .agg(
        f.sum("sold_quantity").alias("sold_quantity")
    )
    .withColumnRenamed("month_start","date")   
)
display(df_monthly.limit(10))


# COMMAND ----------

df_monthly.printSchema()

# COMMAND ----------

df_monthly.count()

# COMMAND ----------

#Now merging child to parent table
gold_parent_delta = DeltaTable.forName(spark,f"{catalog}.{gold_schema}.fact_orders")

gold_parent_delta.alias("parent_gold").merge(
    df_monthly.alias("child_gold"),"parent_gold.date = child_gold.date AND parent_gold.product_code = child_gold.product_code AND parent_gold.customer_code = child_gold.customer_code"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
#Now we will create the final table which is the parent table" 



# COMMAND ----------


# Databricks notebook source
#df=spark.read.csv("path")
#df=spark.read.json("path")
#df=spark.read.parquet("path")

# COMMAND ----------

# DBTITLE 1,Extract  Reading
# step 1: Extract
#df=spark.read.option("header",True).option("inferSchema",True).csv("/Volumes/jpmc/naval/raw/sales.csv")
input_path="/Volumes/workspace/dbs/dbs_volume"
df=spark.read.csv(input_path,header=True,inferSchema=True)

# COMMAND ----------

Lazy evaluation 


Transformations
dataframe functions
.select
.filter
.withColumnRenamed


functions ( from pyspark.sql.functions import col )
col
current_date
upper
lower 



Actions
show
display
write
count


# COMMAND ----------

df1=df.select("order_id","customer_id")

# COMMAND ----------

df.filter("order_id=1").display()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df.select("order_id".alias("orderid"))

# COMMAND ----------

df.select(col("order_id").alias("orderid"))

# COMMAND ----------

df_renamed=df.withColumnRenamed("order_id","orderid")

# COMMAND ----------

df_renamed.display()

# COMMAND ----------

df.withColumn("total_amount",round("total_amount")).display()

# COMMAND ----------

df.withColumn("ingestion_date",current_date()).display()

# COMMAND ----------



# COMMAND ----------

df.write.saveAsTable("dbs.Sales")


# COMMAND ----------

select * from dbs.Sales

# COMMAND ----------

dfp = spark.read.parquet("/Volumes/workspace/dbs/dbs_volume/titanic.parquet")


# COMMAND ----------

dfp.write.saveAsTable("dbs.Titnic_Parquet")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dbs.titnic_parquet

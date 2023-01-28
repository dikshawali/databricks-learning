# Databricks notebook source
df=spark.read.csv('dbfs:/FileStore/samplecsvarray2.csv')

# COMMAND ----------

from pyspark.sql.types import StructType, StringType, StructField

schema=StructType([
StructField ('Month', StringType(), True)
])

# COMMAND ----------

df=spark.read.format('csv').option('mode', 'permissive').option('header', True).load('dbfs:/FileStore/corrupt_record_csv.csv')

# COMMAND ----------

df.withColumn('corrupt_record', StringType(), True)

# COMMAND ----------

list=['My','Name','is','Diksha','wali','and','I','need','a','project','asap']

# COMMAND ----------

list=list.replace(' ', '')

# COMMAND ----------

print(list)

# COMMAND ----------

# MAGIC %sql
# MAGIC create table audit_log
# MAGIC (
# MAGIC   col1 int
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into audit_log values
# MAGIC (1)

# COMMAND ----------

from delta.tables import *

delta_df=DeltaTable.forPath(spark,"/user/hive/warehouse/customer_table_2")

laftoperationdf=delta_df.history(10)
display(laftoperationdf)
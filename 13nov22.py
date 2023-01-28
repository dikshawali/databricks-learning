# Databricks notebook source
df=spark.read.csv('dbfs:/FileStore/samplecsvarray3.csv')

# COMMAND ----------

df.rdd.getNumPartitions()

# COMMAND ----------

info(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table table1
# MAGIC as
# MAGIC select * from csv.`dbfs:/FileStore/samplecsvarray3.csv`

# COMMAND ----------

# MAGIC %sql
# MAGIC describe formatted table1

# COMMAND ----------

spark.conf.get('spark.sql.files.maxPartitionBytes')
spark.conf.get('spark.sql.shuffle.partitions')

# COMMAND ----------

df=spark.read.csv('')
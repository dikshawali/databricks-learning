# Databricks notebook source
spark.conf.get('spark.sql.sources.bucketing.enabled')

# COMMAND ----------

#creating sample dataset
from pyspark.sql.functions import col, rand
df=spark.range(1,1000,1,10).select(col('id').alias('PK'), rand(10).alias('attribute'))


# COMMAND ----------

display(df)

# COMMAND ----------

df.write.saveAsTable('new_table')

# COMMAND ----------

display(df.count())

# COMMAND ----------

df.write.format('parquet').bucketBy(10,'PK').saveAsTable('bucketed_table')

# COMMAND ----------

spark.

# COMMAND ----------

display(df)#=df.withColumnRenamed('PK', 'PK_id')

# COMMAND ----------

import pyspark.sql.functions

df=spark.s

# COMMAND ----------

# MAGIC %sql
# MAGIC select current_database()

# COMMAND ----------

print('hello')

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table left_table
# MAGIC (
# MAGIC   col1 int
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into right_table
# MAGIC values
# MAGIC (1),(2),(3),(4),(null)

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from left_table

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from left_table rt
# MAGIC inner join right_table lt
# MAGIC on rt.col1=lt.col1

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from left_table
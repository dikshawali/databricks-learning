# Databricks notebook source
spark.readStream.format("cloudFiles") \
                  .option("cloudFiles.format", 'csv') \
                  .option("cloudFiles.schemaLocation", 'dbfs:/users/target_table') \
                  .load('dbfs:/FileStore/tables/mydata/annual_enterprise_survey_2021_financial_year_provisional_csv.csv') \
                  .writeStream \
                  .option("checkpointLocation", 'dbfs:/users/checkpoint') \
                  .option("mergeSchema", "true") \
                  .table(table_name)

# COMMAND ----------

df = (spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "json")
      .load('dbfs:/FileStore/tables/mydata/sample2.json'))
 
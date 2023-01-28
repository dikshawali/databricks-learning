# Databricks notebook source
print('hello')

# COMMAND ----------

# DBTITLE 1,Vacuum Command
# MAGIC %fs ls 'dbfs:/FileStore/tables/mydata/annual_enterprise_survey_2021_financial_year_provisional_csv.csv'

# COMMAND ----------

dframe=spark.read.format('csv').option('header', True).option('inferschema', True).load('dbfs:/FileStore/tables/mydata/annual_enterprise_survey_2021_financial_year_provisional_csv.csv')

# COMMAND ----------

display(dframe)

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table Customer_table_2
# MAGIC (
# MAGIC Year  int,
# MAGIC  Industry_aggregation_NZSIOC  string,
# MAGIC  Industry_code_NZSIOC  string,
# MAGIC  Industry_name_NZSIOC  string,
# MAGIC  Units  string,
# MAGIC  Variable_code  string,
# MAGIC  Variable_name  string,
# MAGIC  Variable_category  string,
# MAGIC  Value  string,
# MAGIC  Industry_code_ANZSIC06  string
# MAGIC )
# MAGIC using
# MAGIC delta location 'dbfs:/sample/sample_data'

# COMMAND ----------

spark.sql("""
            create or replace table Customer_table
        (
    Year  int,
 Industry_aggregation_NZSIOC  string,
 Industry_code_NZSIOC  string,
 Industry_name_NZSIOC  string,
 Units  string,
 Variable_code  string,
 Variable_name  string,
 Variable_category  string,
 Value  string,
 Industry_code_ANZSIC06  string
)
using
delta location 'dbfs:/FileStore/tables/mydata/annual_enterprise_survey_2021_financial_year_provisional_csv.csv'
            
          """);

# COMMAND ----------

dframe.write.format("delta").mode("append").option("path", 'dbfs:/sample/sample_data/customer_data').saveAsTable("Customer_table_2")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from customer_table_2

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history Customer_table

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into customer_table
# MAGIC (
# MAGIC   select * from customer_table
# MAGIC   limit 1
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC vacuum Customer_table retain 1 hours dry run

# COMMAND ----------

# MAGIC %sql
# MAGIC optimize Customer_table

# COMMAND ----------

# DBTITLE 1,describe command
# MAGIC %sql
# MAGIC describe history customer_table

# COMMAND ----------

# MAGIC %sql
# MAGIC desc extended Customer_table

# COMMAND ----------

# MAGIC %sql
# MAGIC describe detail Customer_table

# COMMAND ----------

# MAGIC %sql
# MAGIC desc privacy Customer_data

# COMMAND ----------

# DBTITLE 1,Alter - Constraint Command
# MAGIC %sql
# MAGIC ALTER TABLE customer_table
# MAGIC ADD CONSTRAINT
# MAGIC new_constraint
# MAGIC check (year<2021)

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE customer_table
# MAGIC ADD CONSTRAINT
# MAGIC new_constraint2
# MAGIC check (year<2022)

# COMMAND ----------


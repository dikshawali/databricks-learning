-- Databricks notebook source
select 'Hello'

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dframe=spark.read.option("header",True).format('csv').load('dbfs:/FileStore/tables/mydata/annual_enterprise_survey_2021_financial_year_provisional_csv.csv')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dframe)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dframe.createOrReplaceTempView("Customer")

-- COMMAND ----------

select * from Customer
limit 20;

-- COMMAND ----------

-- DBTITLE 1,count_if()
select count_if(year = '2016'), year from Customer
group by year;
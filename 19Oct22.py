# Databricks notebook source
# MAGIC %sql
# MAGIC alter table customer_table_2
# MAGIC add column c1 int;

# COMMAND ----------

# MAGIC %sql
# MAGIC create table new_sample
# MAGIC 
# MAGIC as
# MAGIC select
# MAGIC c1 ,
# MAGIC   Year ,
# MAGIC Industry_aggregation_NZSIOC ,
# MAGIC Industry_code_NZSIOC ,
# MAGIC Industry_name_NZSIOC ,
# MAGIC Units ,
# MAGIC Variable_code ,
# MAGIC Variable_name ,
# MAGIC Variable_category ,
# MAGIC Value ,
# MAGIC Industry_code_ANZSIC06 
# MAGIC from
# MAGIC customer_table_2
# MAGIC limit 20

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from new_sample

# COMMAND ----------

# MAGIC %python
# MAGIC num=0

# COMMAND ----------

# MAGIC %python
# MAGIC num=0;
# MAGIC def getId(num):
# MAGIC 
# MAGIC     num=num+1;
# MAGIC     return num;

# COMMAND ----------

# MAGIC %python
# MAGIC getId_udf=udf(lambda num: getId(num))

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into new_sample
# MAGIC values
# MAGIC (
# MAGIC   getId_udf(num),
# MAGIC   '2021',
# MAGIC   'Level 1',
# MAGIC   '99999',
# MAGIC   'All industries',
# MAGIC   'Dollars (millions)',
# MAGIC 'H21',
# MAGIC 'Opening stocks',
# MAGIC 'Financial performance'
# MAGIC   
# MAGIC   
# MAGIC )

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.functions._

# COMMAND ----------

# MAGIC %python
# MAGIC spark.udf.register("getId_udf", getId(num))

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC server_name = "jdbc:sqlserver://{myserver1345.database.windows.net}"
# MAGIC database_name = "mydb "
# MAGIC url = server_name + ";" + "databaseName=" + database_name + ";"
# MAGIC 
# MAGIC table_name = "SalesLT.Customer"
# MAGIC username = "diksha"
# MAGIC password = "Myserver1345" # Please specify password here
# MAGIC 
# MAGIC try:
# MAGIC   df.write \
# MAGIC     .format("com.microsoft.sqlserver.jdbc.spark") \
# MAGIC     .mode("overwrite") \
# MAGIC     .option("url", url) \
# MAGIC     .option("dbtable", table_name) \
# MAGIC     .option("user", username) \
# MAGIC     .option("password", password) \
# MAGIC     .save()
# MAGIC except ValueError as error :
# MAGIC     print("Connector write failed", error)

# COMMAND ----------

# MAGIC %scala
# MAGIC import java.util.Properties
# MAGIC 
# MAGIC val jdbchostname="myserver1345.database.windows.net"
# MAGIC val jdbcport=1433
# MAGIC val jdbcdbname="mydb"
# MAGIC val myproperties=new Properties()
# MAGIC 
# MAGIC myproperties.put("user", "diksha")
# MAGIC myproperties.put("password", "Myserver1345")
# MAGIC 
# MAGIC val url = s"jdbc:sqlserver://${jdbchostname}:1433;database=${jdbcdbname}"
# MAGIC val driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
# MAGIC myproperties.setProperty("Driver", driverClass)

# COMMAND ----------

# MAGIC %scala
# MAGIC val mydf = spark.read.format("csv")
# MAGIC     .option("header","true")
# MAGIC     .option("inferSchema", "true")
# MAGIC           .load("dbfs:/")

# COMMAND ----------

# MAGIC %scala
# MAGIC import java.sql._
# MAGIC Connection con = DriverManager.getConnection(
# MAGIC                 url, "diksha", "Myserver1345");

# COMMAND ----------



# COMMAND ----------

etl_query = "(" + "select dbo.sample_fun() as fun" + ")ETL"
jdbcUsername = "diksha"
jdbcPassword =  "Myserver1345"
jdbcHostname =  "myserver1345.database.windows.net"
jdbcPort = 1433
jdbcDatabase = "mydb"
jdbcConnectionProperties = {
"driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
 , "user" : jdbcUsername
 , "password" : jdbcPassword
}
jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2};user={3};password={4}".format(jdbcHostname, jdbcPort, jdbcDatabase, jdbcUsername, jdbcPassword)
df = spark.read.jdbc(url=jdbcUrl, properties=jdbcConnectionProperties, table = etl_query)

# COMMAND ----------

display(df)
# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC #hey
# MAGIC 
# MAGIC * First do this.
# MAGIC 1. Then this
# MAGIC 2. Okay fine we are **good** to go.
# MAGIC 3. page by clicking on the ![compute](https://files.training.databricks.com/imlusters-icon.png) icon
# MAGIC 
# MAGIC  **`single node`** cluster

# COMMAND ----------

# MAGIC %md
# MAGIC **`%sql`**
# MAGIC **`SELECT "I'm running SQL!"`**

# COMMAND ----------

DA

# COMMAND ----------

import re

txt = "heoyohello planet heyhello hellohey heyhellohey"

#Check if the string starts with 'hello':

x = re.findall("^hello", txt)
print(x)


# COMMAND ----------

print(f"DA:                   {DA}")
print(f"DA.username:          {DA.username}")
print(f"DA.paths.working_dir: {DA.paths.working_dir}")
print(f"DA.db_name:           {DA.db_name}")

# COMMAND ----------

dbutils.help()

# COMMAND ----------

dbutils.credentials.help()

# COMMAND ----------

dbutils.data.summarize()

# COMMAND ----------

spark.sql("CREATE TABLE recipes (
  recipe_id INT NOT NULL,
  recipe_name VARCHAR(30) NOT NULL,
  PRIMARY KEY (recipe_id),
  UNIQUE (recipe_name)
)")



# COMMAND ----------

dbutils.help()

# COMMAND ----------

dbutils.widgets.text(
  name='your_name_text',
  defaultValue='Enter your name',
  label='Your name'
)

print(dbutils.widgets.get("your_name_text"))

# Enter your name

# COMMAND ----------

a=3

# COMMAND ----------

print(a)
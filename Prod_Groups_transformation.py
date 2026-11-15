# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %run "/Workspace/Users/kishorekumarjh666@gmail.com/Optum/Prod_Folder/Prod_Connectors"

# COMMAND ----------

# MAGIC %run "/Workspace/Users/kishorekumarjh666@gmail.com/Optum/Prod_Folder/Prod_Generic"

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.adlsformedalion.dfs.core.windows.net",
    dbutils.secrets.get(scope="optumdbxscope", key="adlskey"))

# COMMAND ----------

display(dbutils.secrets.listScopes())

# COMMAND ----------

display(dbutils.secrets.list("optumdbxscope"))

# COMMAND ----------

adls_connect()

# COMMAND ----------

list_bronze_files()

# COMMAND ----------

group_df=read_bronze_csv("group")

# COMMAND ----------

display(group_df.limit(20))

# COMMAND ----------

row_columns_count(group_df)

# COMMAND ----------

check_null_values(group_df,group_df.columns)

# COMMAND ----------

check_duplicates(group_df,"premium_written")

# COMMAND ----------

display(group_df.limit(5))

# COMMAND ----------

write2silver(group_df,"Group_s.csv")
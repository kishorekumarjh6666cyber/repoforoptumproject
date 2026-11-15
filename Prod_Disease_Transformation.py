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

disease_df=read_bronze_csv("disease")

# COMMAND ----------

display(disease_df.limit(20))

# COMMAND ----------

row_columns_count(disease_df)

# COMMAND ----------

check_null_values(disease_df,disease_df.columns)

# COMMAND ----------

display(disease_df.limit(5))

# COMMAND ----------

display(disease_df.select("*").filter(col("disease_name").isin("Flu","Bladder cancer")))

# COMMAND ----------

write2silver(disease_df,"Disease_S.csv")
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

adls_connect()

# COMMAND ----------

hos_df=read_bronze_csv("hospital")

# COMMAND ----------

row_columns_count(hos_df)

# COMMAND ----------

hos_df=hos_df.replace("NaN",None)

# COMMAND ----------

check_null_values(hos_df,hos_df.columns)

# COMMAND ----------

check_duplicates(hos_df,"state")

# COMMAND ----------

hos_df=hos_df.fillna({"state":"Union Territory"})
hos_df=hos_df.replace("New Delhi","Delhi")

# COMMAND ----------

write2silver(hos_df,"hospital_s.csv")
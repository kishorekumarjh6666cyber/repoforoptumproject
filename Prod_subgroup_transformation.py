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

adls_connect()

# COMMAND ----------

hos_df=read_bronze_csv("subgroup")

# COMMAND ----------

row_columns_count(hos_df)

# COMMAND ----------

hos_df=hos_df.replace("NaN",None)

# COMMAND ----------

check_null_values(hos_df,hos_df.columns)

# COMMAND ----------

check_duplicates(hos_df,"subgrp_sk")

# COMMAND ----------

hos_df=hos_df.withColumn("subgrp_id",split(col("subgrp_id"),','))

# COMMAND ----------

hos_df=hos_df.withColumn("subgrp_id",explode(col("subgrp_id")))

# COMMAND ----------

write2silver(hos_df,"subGrp_s.csv")
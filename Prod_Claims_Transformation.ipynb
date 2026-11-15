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

claims_df=read_bronze_json("claims")

# COMMAND ----------

display(claims_df.limit(20))

# COMMAND ----------

row_columns_count(claims_df)

# COMMAND ----------

check_null_values(claims_df,claims_df.columns)

# COMMAND ----------

display(claims_df.filter(claims_df.Claim_Or_Rejected == "NaN").agg(count("Claim_Or_Rejected")))

# COMMAND ----------

claims_df=claims_df.replace("NaN",None)

# COMMAND ----------

check_null_values(claims_df,claims_df.columns)

# COMMAND ----------

claims_df=claims_df.fillna({"Claim_Or_Rejected":"N"})

# COMMAND ----------

display(claims_df.limit(5))

# COMMAND ----------

display(claims_df.select("*").filter(col("SUB_ID").isin("SUBID10022","SUBID10049")))

# COMMAND ----------

write2silver(claims_df,"claims_S.csv")
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

subsc_df=read_bronze_csv("subscriber")

# COMMAND ----------

row_columns_count(subsc_df)

# COMMAND ----------

check_null_values(subsc_df,subsc_df.columns)

# COMMAND ----------

#Transformations
subsc_df=subsc_df.fillna({'first_name':'Unknown/NA'})

# COMMAND ----------

check_null_values(subsc_df,subsc_df.columns)

# COMMAND ----------

subsc_df.dtypes

# COMMAND ----------

subsc_df=subsc_df.fillna({"Elig_ind":"N"})

# COMMAND ----------

subsc_df=subsc_df.drop("phone")


# COMMAND ----------

#Need to find patient id for these 2 subscribers
#subgroup ids
#SUBID10022 Patient id =134184....Flu.....Took it from claims tarnsformation after backend engineering
#SUBID10049 patient id = 121783...Bladder cancer
##What disease they have get to know from patient data using patient ids
##S107--> Bladder cancer -->SubGrp_id --> we got this from disease transformation
#S110--> flu

# COMMAND ----------

subsc_df = subsc_df.withColumn(
    "Subgrp_id",
    when(
        col("Subgrp_id").isNull() & (col("sub_id") == "SUBID10022"), "S110"
    )
    .when(
        col("Subgrp_id").isNull() & (col("sub_id") == "SUBID10049"), "S107"
    )
    .otherwise(col("Subgrp_id"))
)

# COMMAND ----------

write2silver(subsc_df,"Subscribers_S.csv")

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


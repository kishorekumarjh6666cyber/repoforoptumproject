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

pat_df=read_bronze_csv("Patient_records")

# COMMAND ----------

row_columns_count(pat_df)

# COMMAND ----------

check_null_values(pat_df,pat_df.columns)

# COMMAND ----------

display(pat_df.limit(5))

# COMMAND ----------

#Transformations
pat_df=pat_df.fillna({'Patient_name':'Unknown/NA'})

# COMMAND ----------

check_null_values(pat_df,pat_df.columns)

# COMMAND ----------

pat_df=pat_df.fillna({'patient_phone':'Not_Available'})

# COMMAND ----------

pat_df=pat_df.withColumn("patient_phone",concat(substring(col("patient_phone"),1,6),lit("*****"),substring(col("patient_phone"),-2,2)))


# COMMAND ----------

pat_df=pat_df.withColumn("Age",(months_between(current_date(),col("patient_birth_date"))/12).cast("Integer"))

# COMMAND ----------

pat_df=pat_df.drop("patient_birth_date")

# COMMAND ----------

write2silver(pat_df,"Patients_s.csv")
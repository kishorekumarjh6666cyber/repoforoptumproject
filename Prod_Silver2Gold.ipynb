# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %run "/Workspace/Users/kishorekumarjh666@gmail.com/Optum/Prod_Folder/Prod_Connectors"

# COMMAND ----------

adls_connect()

# COMMAND ----------

list_silver_files()

# COMMAND ----------

dis_df=read_silver_files("Disease_S")
group_df=read_silver_files("Group_s")
Pat_df=read_silver_files("Patients_s")
Subscr_df=read_silver_files("Subscribers_S")
claims_df=read_silver_files("claims_S")
hospital_df=read_silver_files("hospital_s")
subgrp_df=read_silver_files("subGrp_s")

# COMMAND ----------

c = claims_df.alias("c")
p = Pat_df.alias("p")
h = hospital_df.alias("h")
s = Subscr_df.alias("s")
d = dis_df.alias("d")
sub = subgrp_df.alias("sub")
g = group_df.alias("g")

final_df = c.join(p, col("c.patient_id") == col("p.Patient_id"), "left") \
    .join(h, col("p.hospital_id") == col("h.Hospital_id"), "left") \
    .join(s, col("c.SUB_ID") == col("s.sub_id"), "left") \
    .join(d, col("c.disease_name") == col("d.disease_name"), "left") \
    .join(sub, col("s.Subgrp_id") == col("sub.subgrp_sk"), "left") \
    .join(g, col("sub.subgrp_id") == col("g.grp_id"), "left")

# COMMAND ----------

print(claims_df.columns)
print(Pat_df.columns)
print(hospital_df.columns)
print(Subscr_df.columns)
print(dis_df.columns)
print(subgrp_df.columns)
print(group_df.columns)

# COMMAND ----------

display(final_df.limit(5))

# COMMAND ----------

final_df = final_df.select(
    # Claims
    col("c.claim_id"),
    col("c.claim_date"),
    col("c.claim_amount"),
    col("c.claim_type"),
    col("c.disease_name").alias("claims_disease_name"),
    col("c.Claim_Or_Rejected"),

    # Patient
    col("p.Patient_id").alias("patient_id"),
    col("p.Patient_name"),
    col("p.patient_gender"),
    col("p.patient_phone"),
    col("p.city").alias("patient_city"),
    col("p.Age").alias("patient_age"),

    # Hospital
    col("h.Hospital_id"),
    col("h.Hospital_name"),
    col("h.city").alias("hospital_city"),
    col("h.state").alias("hospital_state"),
    col("h.country").alias("hospital_country"),

    # Subscriber
    col("s.sub_id"),
    col("s.first_name"),
    col("s.last_name"),
    col("s.City").alias("subscriber_city"),
    col("s.Country").alias("subscriber_country"),
    col("s.Zip Code").alias("subscriber_zip_code"),
    col("s.Subgrp_id"),

    # Disease
    col("d.disease_id"),

    # Subgroup
    col("sub.subgrp_sk"),
    col("sub.subgrp_name"),
    col("sub.monthly_premium"),

    # Group
    col("g.grp_id"),
    col("g.grp_name"),
    col("g.grp_type"),
    col("g.city").alias("group_city"),
    col("g.country").alias("group_country"),
    col("g.zip_code").alias("group_zip_code"),
    col("g.premium_written")
)

# COMMAND ----------

display(final_df.limit(5))

# COMMAND ----------

final_df = final_df.fillna("N/A").fillna(0)

# COMMAND ----------

display(final_df.limit(5))

# COMMAND ----------

write2gold(final_df, "final_df.csv")
write2db(final_df, "final_tb")

# COMMAND ----------


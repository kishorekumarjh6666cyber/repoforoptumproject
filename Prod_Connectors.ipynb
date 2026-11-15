# Databricks notebook source
def adls_connect():
    spark.conf.set(
    "fs.azure.account.key.adlsformedalion.dfs.core.windows.net",
    dbutils.secrets.get(scope="optumdbxscope", key="adlskey"))
    return "ADLS connected"

# COMMAND ----------

def list_bronze_files():
    display(dbutils.fs.ls(
    "abfss://stgformedallion@adlsformedalion.dfs.core.windows.net/medallion/Bronze/"))
    return "Bronze files listed"

# COMMAND ----------

def list_silver_files():
    display(dbutils.fs.ls(
    "abfss://stgformedallion@adlsformedalion.dfs.core.windows.net/medallion/silver/"))
    return "Silver files listed"

# COMMAND ----------

def list_gold_files():
    display(dbutils.fs.ls(
    "abfss://stgformedallion@adlsformedalion.dfs.core.windows.net/medallion/gold/"))
    return "Bronze files listed"

# COMMAND ----------

def read_bronze_csv(filename):
    data=spark.read.csv("abfss://stgformedallion@adlsformedalion.dfs.core.windows.net/medallion/Bronze/"+filename+".csv", header=True, inferSchema=True)
    return data

# COMMAND ----------

def read_silver_files(filename):
    data=spark.read.csv("abfss://stgformedallion@adlsformedalion.dfs.core.windows.net/medallion/silver/"+filename+".csv", header=True, inferSchema=True)
    return data

# COMMAND ----------

def read_gold_files(filename):
    data=spark.read.csv("abfss://stgformedallion@adlsformedalion.dfs.core.windows.net/medallion/gold/"+filename+".csv", header=True, inferSchema=True)
    return data

# COMMAND ----------

def read_bronze_json(filename):
    data=spark.read.csv("abfss://stgformedallion@adlsformedalion.dfs.core.windows.net/medallion/Bronze/"+filename+".json",header=True, inferSchema=True)
    return data

# COMMAND ----------

def write2db(df,table_name):
    hostName=dbutils.secrets.get(scope="optumdbxscope", key="azurehostname")
    PortNumber=1433
    DatabaseName=dbutils.secrets.get(scope="optumdbxscope", key="DatabaseName")
    UserName=dbutils.secrets.get(scope="optumdbxscope", key="userName")
    Password=dbutils.secrets.get(scope="optumdbxscope", key="Password")
    urloftarget="jdbc:sqlserver://{0}:{1};database={2}".format(hostName,PortNumber,DatabaseName)
    output=df.write.jdbc(url=urloftarget,table=table_name,mode="overwrite",properties={"user":UserName,"password":Password})
    return "******Successfully written in azure sql database******"

# COMMAND ----------

host_server=dbutils.secrets.get(scope="optumdbxscope", key="azurehostname")

# COMMAND ----------

print(host_server)

# COMMAND ----------

def write2silver(df,filename):
    silver_path = "abfss://stgformedallion@adlsformedalion.dfs.core.windows.net/medallion/silver/"
    temp_path = f"{silver_path}/output_temp"
    final_path = f"{silver_path}/{filename}"
    df.write.mode("overwrite").option("header","true").csv(temp_path)
    files=dbutils.fs.ls(temp_path)
    csv_file=[file.path for file in files if file.path.endswith(".csv")][0]
    dbutils.fs.mv(csv_file,final_path)
    dbutils.fs.rm(temp_path,recurse=True)
    return "******Successfully written in silver layer******"

# COMMAND ----------

def write2gold(df,filename):
    gold_path = "abfss://stgformedallion@adlsformedalion.dfs.core.windows.net/medallion/gold/"
    temp_path = f"{gold_path}/output_temp"
    final_path = f"{gold_path}/{filename}"
    df.write.mode("overwrite").option("header","true").csv(temp_path)
    files=dbutils.fs.ls(temp_path)
    csv_file=[file.path for file in files if file.path.endswith(".csv")][0]
    dbutils.fs.mv(csv_file,final_path)
    dbutils.fs.rm(temp_path,recurse=True)
    return "******Successfully written in gold layer******"

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


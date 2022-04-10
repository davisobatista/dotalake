# Databricks notebook source
import requests

# COMMAND ----------

url = "https://api.opendota.com/api/proMatches"

# COMMAND ----------

response = requests.get(url)

# COMMAND ----------

response.json()

# COMMAND ----------

#Criar dataframe a partir do json
df = spark.createDataFrame(response.json())

# COMMAND ----------

#exibir o dataframe em spark
df.display()

# COMMAND ----------

aws_path = "/mnt/datalake"

# COMMAND ----------

#listar o q ha na pasta
dbutils.fs.ls("/mnt/datalake")

# COMMAND ----------

dbutils.fs.mkdirs(aws_path + "/raw")

# COMMAND ----------

aws_path = aws_path + "/raw/"

# COMMAND ----------

df.repartition(1).write.format("json").mode("append").save(aws_path + "pro_matches.history")

# COMMAND ----------

#listar o q ha na pasta
dbutils.fs.ls(aws_path + "pro_matches.history")

# COMMAND ----------



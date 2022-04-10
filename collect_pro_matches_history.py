# Databricks notebook source
import requests

# COMMAND ----------

url = "https://api.opendota.com/api/proMatches"

# COMMAND ----------

response = requests.get(url)

# COMMAND ----------

response.json()

# COMMAND ----------

df = spark.createDataFrame(response.json())

# COMMAND ----------

df.display()

# COMMAND ----------



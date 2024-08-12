# Databricks notebook source
dbutils.widgets.text("catalog", "stuart", "Default UC catalog")
dbutils.widgets.text("schema", "ts", "Default UC schema")

# COMMAND ----------

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")

# COMMAND ----------

from pyspark.sql import DataFrame
import pyspark.sql.functions as F
import pyspark.sql.window as W

# COMMAND ----------

interval_types = ["years", "quarters", "months"]

ws = W.Window.partitionBy("cdid", "ts").orderBy(F.desc("updateDate"))

for it in interval_types:
  (
    spark.table(f"{CATALOG}.{SCHEMA}.silver_{it}")
    .withColumn("rn", F.row_number().over(ws))
    .where(F.col("rn") == 1)
    .drop("rn")
    .write
    .mode("overwrite")
    .saveAsTable(f"{CATALOG}.{SCHEMA}.gold_{it}")
  )

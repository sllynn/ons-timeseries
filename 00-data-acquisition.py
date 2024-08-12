# Databricks notebook source
dbutils.widgets.text("catalog", "stuart", "Default UC catalog")
dbutils.widgets.text("schema", "ts", "Default UC schema")
dbutils.widgets.text("time_series_ids", "D7BT,DKC6,DK9J,D7F5,D7BU", "Time series IDs, comma separated")

# COMMAND ----------

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

# COMMAND ----------

import requests
import pandas as pd
import pyspark.sql.functions as F

from requests.adapters import HTTPAdapter, Retry

s = requests.Session()

retries = Retry(total=5, backoff_factor=1, status_forcelist=[ 502, 503, 504, 429 ])
s.mount('https://www.ons.gov.uk', HTTPAdapter(max_retries=retries))

# COMMAND ----------

# l4oj
# grossvalueaddedgva

ts_ids = dbutils.widgets.get("time_series_ids").split(",")
urls = [f"https://www.ons.gov.uk/economy/grossvalueaddedgva/timeseries/l4oj/wgdp/data" for id in ts_ids]
urls

# COMMAND ----------

ts_ids = dbutils.widgets.get("time_series_ids").split(",")
urls = [f"https://www.ons.gov.uk/economy/inflationandpriceindices/timeseries/{id.strip().lower()}/mm23/data" for id in ts_ids]
urls

# COMMAND ----------

data = [requests.get(url).json() for url in urls]

# COMMAND ----------

raw_df = spark.createDataFrame(pd.DataFrame(data))
raw_df.display()

# COMMAND ----------

(
  raw_df
  .drop("sourceDatasets")
  .write
  .mode("overwrite")
  .saveAsTable(f"{CATALOG}.{SCHEMA}.raw")
  )

# COMMAND ----------

# MAGIC %sql
# MAGIC COMMENT ON TABLE stuart.ts.raw IS "Time series data for CPI indices from ONS. Contains last 10 years of data."

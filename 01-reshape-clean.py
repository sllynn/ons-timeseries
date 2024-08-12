# Databricks notebook source
dbutils.widgets.text("catalog", "stuart", "Default UC catalog")
dbutils.widgets.text("schema", "ts", "Default UC schema")

# COMMAND ----------

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")

# COMMAND ----------

from pyspark.sql import DataFrame
import pyspark.sql.functions as F

# COMMAND ----------

raw_df = spark.table(f"{CATALOG}.{SCHEMA}.raw")

# COMMAND ----------

ts_key_cols = ["cdid", "date", "releaseDate"]
metadata_cols = [
  "uri", "type", "description",
  "relatedDatasets", "relatedDocuments", "relatedData",
  "versions"
  ]

base_df = raw_df

for col in ts_key_cols:
  base_df = base_df.withColumn(col, F.col("description").getField(col))
base_df = base_df.drop(*metadata_cols)
base_df.display()

# COMMAND ----------

# Create a UDF to convert "YYYY QQ" and "YYYY MM" to "YYYY-MM-DD"
def label_to_timestamp(date_str):
  label_to_month = {
    "Q1": "01",
    "Q2": "04",
    "Q3": "07",
    "Q4": "10",
    "JAN": "01", "FEB": "02", "MAR": "03",
    "APR": "04", "MAY": "05", "JUN": "06",
    "JUL": "07", "AUG": "08", "SEP": "09",
    "OCT": "10", "NOV": "11", "DEC": "12"
  }
  year, label = date_str.split()
  month = label_to_month[label]
  return f"{year}-{month}-01"

label_to_timestamp_udf = F.udf(label_to_timestamp)

# COMMAND ----------

interval_types = ["years", "quarters", "months"]
ts_col_schema = {
  "date":           "string",
  "label":          "string",
  "month":          "string",
  "quarter":        "string",
  "sourceDataset":  "string",
  "updateDate":     "timestamp",
  "value":          "double",
  "year":           "integer",
}

def extract_ts(df: DataFrame, interval_type: str) -> DataFrame:
  return (
    df
    .select(*ts_key_cols, interval_type)
    .withColumnRenamed("date", "publishedPeriod")
    .withColumn("ts", F.explode(interval_type))
    .drop(*interval_types)
    .select("*", *[F.col(f"ts.{k}").cast(v).alias(k) for k, v in ts_col_schema.items()])
    .drop("ts", "date")
    .withColumn("ts", F.concat(F.col("year"), F.lit("-01-01")) if interval_type == "years" else label_to_timestamp_udf("label"))
    .withColumn("ts", F.col("ts").cast("timestamp"))
  )

output_dfs = [base_df.transform(extract_ts, i) for i in interval_types]
output_dfs[2].display()

# COMMAND ----------

for it, df in zip(interval_types, output_dfs):
  df.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.silver_{it}")

# COMMAND ----------

metadata_df = raw_df.select(*metadata_cols)

for col in ts_key_cols:
  metadata_df = metadata_df.withColumn(col, F.col("description").getField(col))

metadata_df.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.silver_metadata")

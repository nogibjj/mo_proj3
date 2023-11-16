# Databricks notebook source
# run and import functions from other notebooks
%run ../extract/extract
%run ../transform/transform_data
%run ../load_datalake/

# COMMAND ----------


import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, dayofmonth, month, year
from pyspark.sql.functions import date_format, to_timestamp
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from extract import extract

# COMMAND ----------

# MAGIC %md Extract the data
# MAGIC

# COMMAND ----------

# Example usage
input_file_location = "/FileStore/tables/lacity_org_website_traffic.csv"
file_type = "csv"
infer_schema = "false"
first_row_is_header = "true"
delimiter = ","
data = extract(input_file_location, file_type, infer_schema, first_row_is_header, delimiter)


# COMMAND ----------

# MAGIC %md Transform the data
# MAGIC

# COMMAND ----------

transformed_data = transform_date_columns(data)
transformed_data = transform_data(transformed_data)
transformed_data = cast_columns(transformed_data)

# COMMAND ----------

# MAGIC %md Load to the data lake
# MAGIC

# COMMAND ----------

file_type = "delta"
first_row_is_header = "true"
delimiter = ","
write_to_deltalake(transformed_data, output_file_location, file_type, first_row_is_header, delimiter)

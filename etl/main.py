'''
This is the main script for the ETL pipeline.
It will extract the data from the source, transform it, and load it to the data lake.
'''
import sys
import os
from pyspark.sql import SparkSession
from extract.extract import extract
from transform.transform_data import (transform_date_columns,
                                      transform_data, cast_columns)

sys.path.append(os.path.abspath('../extract/'))
sys.path.append(os.path.abspath('../transform/'))


# COMMAND ----------

spark = SparkSession.builder.appName("etl").getOrCreate()

# COMMAND ----------

# MAGIC %md Extract the data
# MAGIC

# COMMAND ----------

# Example usage
input_file_location = "/FileStore/tables/lacity_org_website_traffic.csv"
output_file_location = "/FileStore/tables/lacity_org_website_traffic_delta"
file_type = "csv"
infer_schema = "false"
first_row_is_header = "true"
delimiter = ","
data = extract(input_file_location, file_type, infer_schema,
               first_row_is_header, delimiter)

# COMMAND ----------

# MAGIC %md Transform the data
# MAGIC

# COMMAND ----------

transformed_data = transform_date_columns(data)
transformed_data = transform_data(transformed_data)
transformed_data = cast_columns(transformed_data)

# COMMAND ----------

# display top 10 rows
#display(transformed_data.limit(10))

# COMMAND ----------

# MAGIC %md Load to the data lake
# MAGIC

# COMMAND ----------

file_type = "delta"
first_row_is_header = "true"
delimiter = ","
# Uncoment the following line to write to Delta Lake databricks
# write_to_deltalake(transformed_data, output_file_location, file_type,
#                    first_row_is_header, delimiter)

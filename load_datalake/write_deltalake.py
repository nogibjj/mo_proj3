# Databricks notebook source
from pyspark.sql import SparkSession

def write_to_deltalake(data, output_file_location, file_type, first_row_is_header, delimiter):
    """
    Writes data to an Azure Delta Lake.

    Parameters:
        data (pyspark.sql.DataFrame): The DataFrame to be written.
        output_file_location (str): The path to the output Delta Lake location.
        file_type (str): The type of the output file (e.g., "delta").
        first_row_is_header (str): Whether the first row of the data is a header.
        delimiter (str): The delimiter used in the data file.

    Returns:
        None
    """
    # Create a Spark session
    spark = SparkSession.builder.appName("write_to_deltalake").getOrCreate()

    # Write output to Delta Lake
    data.write.format(file_type).mode("overwrite").option("header", first_row_is_header).option("sep", delimiter).save(output_file_location)

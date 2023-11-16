# Databricks notebook source
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, dayofmonth, month, year
from pyspark.sql.functions import date_format, to_timestamp
from pyspark.sql.window import Window
from pyspark.sql import functions as F

def extract(input_file_location, file_type, infer_schema="false", first_row_is_header="true", delimiter=","):
    """
    Reads data from a specified file location using PySpark.

    Parameters:
        input_file_location (str): The path to the input file.
        file_type (str): The type of the input file (e.g., "csv").
        infer_schema (str): Whether to infer the schema of the data. Default is "false".
        first_row_is_header (str): Whether the first row of the data is a header. Default is "true".
        delimiter (str): The delimiter used in the data file. Default is ",".

    Returns:
        pyspark.sql.DataFrame: A DataFrame containing the read data.
    """
    # Spark Session Creation
    spark = SparkSession.builder.appName("proj3").getOrCreate()

    # Read the data
    data = (
        spark.read.format(file_type)
        .option("inferSchema", infer_schema)
        .option("header", first_row_is_header)
        .option("sep", delimiter)
        .load(input_file_location)
    )

    return data


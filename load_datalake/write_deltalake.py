# Databricks notebook source
from delta.tables import *
from pyspark.sql import SparkSession

def write_to_deltalake(data, output_file_location):
    """
    Writes data to an Azure Delta Lake.
    
    Parameters:
    data (pyspark.sql.DataFrame): The DataFrame to be written. 
    output_file_location (str): The path to the output Delta Lake location.

    """
    
    # Create a Spark session 
    spark = SparkSession.builder.appName("write_to_deltalake").getOrCreate()
    
    # Convert DataFrame to a DeltaTable
    data.write.format("delta").mode("overwrite").save(output_file_location)

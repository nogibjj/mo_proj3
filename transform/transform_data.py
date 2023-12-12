from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format, to_timestamp
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql.functions import (col, to_date, dayofmonth, month, year, 
	date_format, to_timestamp, row_number, rank, dense_rank, count, first, 
	last, min, max, nth_value, lag, lead, percent_rank, ntile)

spark = SparkSession.builder.appName("etl").getOrCreate()

def transform_date_columns(df):
    df = df.withColumn("timestamp", col("Date")).drop("Date")
    df = df.withColumn("date", to_date(col("timestamp")))
    df = df.withColumn("day", dayofmonth(col("date")))
    df = df.withColumn("month", month(col("date")))  
    df = df.withColumn("year", year(col("date")))
    return df

def transform_data(df):
    df.createOrReplaceTempView("la_tr")
    df_transformed = spark.sql("""
        SELECT *, 
               date_format(to_timestamp(timestamp),"H") AS hour,
               date_format(to_timestamp(timestamp),"E") AS dayofweek,
               row_number() OVER (PARTITION BY `Device Category` ORDER BY timestamp) AS row_num,
               rank() OVER (PARTITION BY `Device Category` ORDER BY timestamp) AS rank,
               dense_rank() OVER (PARTITION BY `Device Category` ORDER BY timestamp) AS dense_rank,
               first(`# of Visitors`) OVER (PARTITION BY `Device Category` ORDER BY timestamp) AS first,
               last(`# of Visitors`) OVER (PARTITION BY `Device Category` ORDER BY timestamp) AS last,
               min(`# of Visitors`) OVER (PARTITION BY `Device Category` ORDER BY timestamp) AS min,
               max(`# of Visitors`) OVER (PARTITION BY `Device Category` ORDER BY timestamp) AS max,
               nth_value(`# of Visitors`, 2) OVER (PARTITION BY `Device Category` ORDER BY timestamp) AS nth,
               lag(`# of Visitors`, 1) OVER (PARTITION BY `Device Category` ORDER BY timestamp) AS lag,
               lead(`# of Visitors`, 1) OVER (PARTITION BY `Device Category` ORDER BY timestamp) AS lead,
               percent_rank() OVER (PARTITION BY `Device Category` ORDER BY timestamp) AS percent,
               ntile(2) OVER (PARTITION BY `Device Category` ORDER BY timestamp) AS ntile
        FROM la_tr
        ORDER BY `Device Category`, timestamp
    """)
    return df_transformed

def cast_columns(df):
    df = df.withColumn("Sessions", df["Sessions"].cast("int"))
    df = df.withColumn("# of Visitors", df["# of Visitors"].cast("int"))
    df = df.withColumn("Bounce Rate", df["Bounce Rate"].cast("int"))
    return df
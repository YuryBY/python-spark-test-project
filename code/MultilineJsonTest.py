"""MultilineJsonTest.py"""
from __future__ import print_function
from pyspark.sql import SparkSession

logFile = "resources/databricks_json_example.json"  # Should be some file on your system
spark = SparkSession.builder.appName("Multiline Json Test").getOrCreate()
logData = spark.read.text(logFile).cache()

logData.foreach(print)

spark.stop()

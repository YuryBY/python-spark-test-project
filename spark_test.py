"""SimpleApp.py"""
from __future__ import print_function
from pyspark.sql import SparkSession

logFile = "databricks_json_example.json"  # Should be some file on your system
spark = SparkSession.builder.appName("SimpleApp").getOrCreate()
logData = spark.read.text(logFile).cache()

numAs = logData.filter(logData.value.contains('a')).count()
numBs = logData.filter(logData.value.contains('b')).count()

logData.foreach(print)

#print("Lines with a: %i, lines with b: %i" % (numAs, numBs))

spark.stop()
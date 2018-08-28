"""MultilineJsonTest1.py"""
from pyspark.sql import SparkSession

databricks_json_path = "databricks_json_example.json"  # Should be some file on your system
spark = SparkSession.builder.appName("DatabricksJsonParse").getOrCreate()
#logData = spark.read.text(databricks_json)
    #.cache()

jsonRDD = spark.textFile(databricks_json_path)

numAs = jsonRDD.filter(jsonRDD.value.contains('a')).count()
numBs = jsonRDD.filter(jsonRDD.value.contains('q')).count()

print("Lines with a: %i, lines with b: %i" % (numAs, numBs))

spark.stop()
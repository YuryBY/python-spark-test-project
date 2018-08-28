from pyspark import SparkContext, SparkConf
spark_conf = SparkConf().setAppName("multiline json")
spark_context = SparkContext(conf=spark_conf)

#DATABRICKS_JSON_PATH = "src/test/resources/databricks_json_example.json"
DATABRICKS_JSON_PATH = "test.py"
#address = "/path/to/the/log/on/hdfs/*.gz"

def print_funct(l):
    print(l)
    return l

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("SimpleApp").getOrCreate()
jsonRDD = spark.read.text(DATABRICKS_JSON_PATH).cache()
#jsonRDD = spark_context.textFile(DATABRICKS_JSON_PATH)
#jsonRDD.collect().foreach(println)

jsonRDD.map(lambda l: print_funct(l)).collect()
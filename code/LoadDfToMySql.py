"""LoadDfToMySql.py"""
from pyspark.sql import SQLContext

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("LoadDFtoMySQL").getOrCreate()
sqlContext = SQLContext(spark.sparkContext)

columns = ['idpy_spark_test', 'job_name', 'running_time']
vals = [
    ('8', 'eighth', '2018-07-02 03:14:08')
]

df = sqlContext.createDataFrame(vals, columns)

df.write.format("jdbc").options(
    url="jdbc:mysql://localhost:3306/foodmart?autoReconnect=true&useSSL=false",
    driver="com.mysql.jdbc.Driver",
    dbtable="py_spark_test",
    user="root",
    password="root"
).mode("append").save()

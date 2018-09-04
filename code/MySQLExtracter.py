"""MySQLExtracter.py"""
from displayfunction import display
from pyspark.sql import SparkSession
# from pyspark.sql import HiveContext, Row
from pyspark.sql import SQLContext, Row

jdbcHostname = "localhost"
jdbcDatabase = "foodmart"
jdbcPort = 3306
username = "root"
password = "root"
jdbcUrl = "jdbc:mysql://{0}:{1}/{2}?user={3}&password={4}?autoReconnect=true&useSSL=false".format(jdbcHostname, jdbcPort, jdbcDatabase, username, password)
connectionProperties = {
  "user" : username,
  "password" : password,
  "driver" : "com.mysql.jdbc.Driver"
}
pushdown_query = "(select * from employee where employee_id < 10) emp_alias"

spark = SparkSession.builder.appName("MySQLExtracter").getOrCreate()

# testDf = spark.read.jdbc(url=jdbcUrl, table='py_spark_test', column='job_name', lowerBound=1, upperBound=100000, numPartitions=100)
# testDf = "jdbc:mysql://{0}:{1}/{2}?user={3}&password={4}?autoReconnect=true&useSSL=false".format(jdbcHostname, jdbcPort, jdbcDatabase, username, password)
testDf = spark.read.jdbc(url=jdbcUrl, table="py_spark_test", properties=connectionProperties)
# testDf = spark.read.jdbc(url=jdbcUrl, table='py_spark_test', column='job_name', lowerBound=1, upperBound=100000, numPartitions=100)
testDf.select("job_name").show()

# testDf.write.mode("append").saveAsTable("py_spark_test")
appendSql = spark.sql("INSERT INTO py_spark_test VALUES('5', 'fifth', '2018-07-19 03:14:08')");
appendSql.write.mode("append").saveAsTable("py_spark_test")
testDf.select("job_name").show()

# data = hc.sql("select 1 as id, 10 as score")
# data.write.mode("append").saveAsTable("my_table")
# display(df)

# df.select("job_name").show()
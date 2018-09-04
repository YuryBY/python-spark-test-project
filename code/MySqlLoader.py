"""MySqlLoader.py"""
from displayfunction import display
from pyspark.sql import SparkSession

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

df = spark.read.jdbc(url=jdbcUrl, table=pushdown_query, properties=connectionProperties)
df.select("full_name").show()
display(df)


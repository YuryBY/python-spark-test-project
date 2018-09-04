"""JdbcToHdfsLoader.py"""
from pyspark.sql import SparkSession

if __name__ == "__main__":
    # possible modes: append(a) and write(w)
    __writeAccumulatorFileMode = "a"

    # create Spark context with Spark configuration
    spark = SparkSession.builder.appName("LoadJdbcTableToHdfs").getOrCreate()

    # arguments
    jdbcHostname = "localhost"
    jdbcDatabase = "foodmart"
    jdbcPort = 3306
    username = "root"
    password = "root"
    table = "employee"
    condition_field = "employee_id"
    condition_value = "100"
    jdbcUrl = "jdbc:mysql://{0}:{1}/{2}?user={3}&password={4}?autoReconnect=true&useSSL=false".format(jdbcHostname,
                                                                                                      jdbcPort,
                                                                                                      jdbcDatabase,
                                                                                                      username,
                                                                                                      password)
    connectionProperties = {
        "user": username,
        "password": password,
        "driver": "com.mysql.jdbc.Driver"
    }

    pushdown_query = "(select * from " + table + " where " + condition_field + "<" + condition_value + ") " + table + "_alias"
    dataFrame = spark.read.jdbc(url=jdbcUrl, table=pushdown_query, properties=connectionProperties)

    try:
        accumulatorFile = open("accumulator", __writeAccumulatorFileMode)
        try:
            accumulatorFile.write(condition_value + "\n")
        finally:
            accumulatorFile.close()
    except IOError:
        print "writing accumulator file error!"

    # df.select("full_name").show()
    # "hdfs://..."
    #     append
    dataFrame.write.mode("overwrite").partitionBy("hire_date", "birth_date").csv("LoadJdbcTableToHdfsOutput.csv")
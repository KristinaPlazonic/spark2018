from pyspark.sql import SparkSession

spark = SparkSession \
        .builder \
        .appName("pyspark-example-helloworld") \
        .getOrCreate()
        #.config("spark.sql.warehouse.dir", "/some/bogus/path") \

### your code here
#print("spark.executor.cores is {}".format(spark.conf.get("spark.executor.cores")))
#print("spark.executor.memory is {}".format(spark.conf.get("spark.executor.memory")))
print("spark.driver.memory is {}".format(spark.conf.get("spark.driver.memory")))
print("spark.sql.warehouse.dir is {}".format( spark.conf.get("spark.sql.warehouse.dir")))

spark.stop()


from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession \
        .builder \
        .appName("pyspark-example-helloworld") \
        .getOrCreate()
        #.config("spark.sql.warehouse.dir", "/some/bogus/path") \

spark.sparkContext.setLogLevel("WARN") ## turn off spurious logging

### your code here
filename = "/home/kris/spark/spark-2.3.0-bin-hadoop2.7/examples/src/main/resources/employees.json"
df = spark.read.json(filename).cache()
print("this is schema self-discovery")
df.show()
df.unpersist()

mydefschema = StructType([StructField("name",StringType(),True), StructField("newsalary",LongType(),True)])
df = spark.read.json(filename, schema=mydefschema).cache()
print("this is schema imposition")
df.show()
df.unpersist()

mydefschema2 = StructType([ StructField("name",StringType(),True) ])
df = spark.read.json(filename, schema=mydefschema2).cache()
print("this is schema imposition - we don't have to read everything")
df.show()
df.unpersist()

spark.stop()



from pyspark.sql import *
df = spark.read.csv("/home/kris/datasets/small/iris.csv")
df.show()
df = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/kris/datasets/small/iris.csv")

df.registerTempTable("mydf")
sql("SELECT SUM(sepal_length) FROM mydf").show()
sql("SELECT SUM(sepal_length) AS sum_sep_len FROM mydf").show()
df.schema
df.printSchema()



from operator import add

lines = spark.read.text("/home/kris/datasets/text/jane_austen.txt")
lines.show(10, False)
linesrdd = lines.rdd
countsrdd = linesrdd.flatMap(lambda x: x.split(' ')) \
              .map(lambda x: (x, 1)) \
              .reduceByKey(add)

counts = lines.flatMap(lambda x: x.split(' ')) \
              .map(lambda x: (x, 1)) \
              .reduceByKey(add)


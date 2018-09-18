
# In interactive sessions, spark is a variable of class SparkSession
spark       # to get the class name
dir(spark)  # to get methods

>>> sc = spark.sparkContext
>>> sc.
sc.PACKAGE_EXTENSIONS    sc.broadcast(            sc.getConf(              sc.parallelize(          sc.serializer            sc.sparkHome             sc.version
sc.accumulator(          sc.cancelAllJobs(        sc.getLocalProperty(     sc.pickleFile(           sc.setCheckpointDir(     sc.sparkUser(            sc.wholeTextFiles(
sc.addFile(              sc.cancelJobGroup(       sc.getOrCreate(          sc.profiler_collector    sc.setJobDescription(    sc.startTime
sc.addPyFile(            sc.defaultMinPartitions  sc.hadoopFile(           sc.pythonExec            sc.setJobGroup(          sc.statusTracker(
sc.appName               sc.defaultParallelism    sc.hadoopRDD(            sc.pythonVer             sc.setLocalProperty(     sc.stop(
sc.applicationId         sc.dump_profiles(        sc.master                sc.range(                sc.setLogLevel(          sc.textFile(
sc.binaryFiles(          sc.emptyRDD(             sc.newAPIHadoopFile(     sc.runJob(               sc.setSystemProperty(    sc.uiWebUrl
sc.binaryRecords(        sc.environment           sc.newAPIHadoopRDD(      sc.sequenceFile(         sc.show_profiles(        sc.union(


rdd = sc.parallelize([1,2,3,4])   # create RDD from a list
rdd.collect() 
mylist = range(0, 10**8)          # a lot of elements
rddlarge = sc.parallelize(mylist)

from operator import add

import time

## elapsed time 
st = time.clock()   #time in seconds
rddlarge.map(lambda x: x*x).reduce(add)
et = time.clock()
print( "elapsed time is {} seconds".format(et - st))  # elapsed time

from functools import reduce
st = time.clock()
reduce( add, map(lambda x: x*x, mylist))
et = time.clock()
print( "elapsed time is {} seconds".format(et - st))  # elapsed time

### filter examples: 
rddlarge.filter( lambda x: x % 1000 == 0).take(10)
filter(lambda x: x % 100000 == 0, mylist)  #not what you expect... 

############### text file - transformations and actions
rddtext = sc.textFile("/home/kris/datasets/text/jane_austen.txt")

rddwords = rddtext.map(lambda ll: ll.split(" "))
for i in rddwords.take(10): 
   print(i)

rddwords2 = rddwords.flatMap(lambda x: x)
for i in rddwords2.take(10): 
   print(i)


############### make a dataframe
bb = spark.createDataFrame(data=[(1,2, 'this', 10.0), (3,4, 'Is', 3.14)], schema=("A", "B", "word", "somedouble"))  ## from some 
bb.withColumn("E", bb.A + bb.B).show()
bb.filter(bb.word == "Is" ).show()
bb[bb.word.startswith("I")]

from pyspark.sql.functions import col
bb.filter(col('word').startswith("I")).show()
bb.filter(bb.word.startswith("I")).show()


############### to read a dataframe
df = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/kris/datasets/small/iris.csv")
df.cache()
df.count()
# now look at the Storage tab in the Spark UI

df.select("petal_length", "*").show()
df.selectExpr("petal_length + sepal_length AS tot_length").show()    ### this is when you have floats instead of decimals!

############### to use a dataframe in SQL statements: 
df.registerTempTable("iris")
spark.sql("select avg(petal_length) AS avg_petal_length from iris").show()

##############  UDF functions
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
slen = udf(lambda s: s.split("-")[1], StringType())   #Iris-setosa --> setosa
df.select(slen("species").alias("species2")).show()
df.withColumn(slen("species").alias("species2")).show()

sqlContext.registerFunction("species_suffix", slen)
df.selectExpr("*", "species_suffix(species AS species2")).show()

### entire group by statement: 
df.select("petal_length", "species").filter("petal_length>3").groupBy("species").count().sort("count", ascending=False).show()
df.filter("petal_length>3").groupBy("species").mean().show() 


### joins

valuesA = [('Alice',25),('Bob',32),('Charlie',34),('Dave',24)]
dfage = spark.createDataFrame(valuesA,['name','age']).cache()
valuesB = [('Alice',"Research"),('Bob',"Sales"),('Charlie',"Sales"),('Evan',"Marketing")]
dfdept = spark.createDataFrame(valuesB,['name','dept']).cache()

dfage.join(dfdept).show()
dfage.join(dfdept, "name").show()
dfage.join(dfdept, "name", how = "left").show()
dfage.join(dfdept, "name", how = "right").show()
dfage.join(dfdept, "name", how = "outer").show()

dfage.withColumnRenamed("name", "name2").join(dfdept, col("name") == col("name2")).show()


############### some bugs

rdd.toDF().show()    #fails 
rdd.zipWithIndex().toDF().show()

########### another

df2 = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/kris/rutgers/teaching/spark/spark_module/Lecture2_sql/churn.csv")
df2.cache()
df2.count()


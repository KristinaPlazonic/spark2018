
## Today: 

- where you should go and read news (Cloudera and Hortonworks merged)
- review of joins - whathever we didn't do last time
- Hive DDLs 
- Homework - work in pairs
- submitting a batch job from CLI
- tuning SparkSQL jobs
- cheatsheet

## Hive

- This is most like a usual SQL database
- To run on CS system, see https://services.cs.rutgers.edu/hive.html. 
- ```HDP_VERSION=2.6.3.0-235 beeline -u "jdbc:hive2://data-services2.cs.rutgers.edu:2181,data-services3.cs.rutgers.edu:2181,data1.cs.rutgers.edu:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2"```  (HDP_VERSION is necessary to find the correct library


### Example DDL file
Example of data that exists apart from Hive in some external table. On top of this file we can impose some schema, that will be stored in Hive metastore. 
```
CREATE EXTERNAL TABLE iris3(
   sepal_length float, 
   sepal_width float,
   petal_length float,
   petal_width float, 
   species string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/kp807/smalldata/iris_noheaders.csv'; 
```
This should work, but it doesn't, so instead of an external table, let's create an internal table and load data in there, closely following instructions on CS site:
TODO! 


## Running a batch script

```
SSPATH="/home/kris/spark/spark-2.3.0-bin-hadoop2.7/bin/spark-submit"
$SSPATH --master local[*]  --driver-memory 8G --executor-memory 8G pyspark_hellowordl.py    #works!
$SSPATH --master local[*]  --properties-file myconffile.conf pyspark_hellowordl.py
```
## Configuration options

See full list at https://spark.apache.org/docs/latest/configuration.html

```
  --master MASTER_URL         spark://host:port, mesos://host:port, yarn,
                              k8s://https://host:port, or local (Default: local[*]).

  --py-files PY_FILES         Comma-separated list of .zip, .egg, or .py files to place
                              on the PYTHONPATH for Python apps.

  --files FILES               Comma-separated list of files to be placed in the working
                              directory of each executor. File paths of these files
                              in executors can be accessed via SparkFiles.get(fileName).

  --conf PROP=VALUE           Arbitrary Spark configuration property.

  --properties-file FILE      Path to a file from which to load extra properties. If not
                              specified, this will look for conf/spark-defaults.conf.
```

## Bugs

- If you set the configuration options the wrong way, it will ignore the setting. E.g. you must set the 
```
$SSPATH --master local[*]   --executor-memory 8G --driver.memory 8G pyspark_hellowordl.py   #doesn't work
$SSPATH --master local[*]  --driver-memory 8G --executor-memory 8G pyspark_hellowordl.py    #works!

```

```
$SSPATH --master local[*]  --conf spark.driver.memory=8G pyspark_hellowordl.py     #works
$SSPATH --master local[*]  pyspark_hellowordl.py --conf spark.driver.memory=8G     #doesn't work
```

You must pay attention which properties you are setting depending on your mode of deployment. With standalone scheduler, some options are not valid. E.g. --total-executor-cores is available only in YARN. 


## Definining schema

- from scratch, as builder:  `StructType().add("bla", LongType()).add("newblah", StringType())` 
- from scratch, as list:   `mydefschema = StructType([StructField("name",StringType(),True), StructField("newsalary",LongType(),True)])

# Homework

The end result of this sequence of tutorials is to build a working pipeline involving Spark. This can be on batch data or on streaming data (extra points for that!). 
For now, let's concentrate how to build a batch application. So here is your homework for next time: 

1. Starting from airline and flights data, build a pyspark app that finds the airports to avoid - i.e. 5 airports with the largerst number of delayed flights. Your app should involve a join and should write the result as json file. 

2. Run this app by varying some configurations when running the app: 
- run in Spark Standalone
- run in YARN
- change `spark.sql.shuffle.partitions` and observe the difference in output files (number of partitions)
- change driver.memory and executor.memory and find out how to change total cores and rerun the app to see how the execution changes (in terms of time)

3. (optional) read ahead about Spark Streaming

4. (optional) read ahead about Machine Learning



## Today: 

- where you should go and read news (Cloudera and Hortonworks merged)
- review of joins - whathever we didn't do last time
- Hive DDLs 
- Homework - work in pairs
- submitting a batch job from CLI
- tuning SparkSQL jobs
- cheatsheet


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
`

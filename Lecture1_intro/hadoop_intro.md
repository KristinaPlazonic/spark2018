## Overview/Agenda

- some Hadoop history
- HDFS in action
- MapReduce in principle
- Spark in action
- comparing WordCount in MapReduce, in Spark, in Pig
- installing your software

## Big Data

Today's data is very big: 
- a single human genome can be 200Gb 
- a single high resolution microscopy image can be in Gb (and you potentially need thousands/millions)
- not uncommon for a large company to deal with a billion rows of data
- a single LSST astronomy experiment - 20 terabytes (TB) raw data per night

## The 3 V's

- volume
- velocity
- variety
- (veracity)
- (show IBM infographic) - https://www.ibmbigdatahub.com/sites/default/files/styles/xlarge-scaled/public/infographic_image/4-Vs-of-big-data.jpg?itok=4syrvSLX

## Hadoop

- 2 meanings: 
   1. Stricter meaning = distributed file system HDFS (Hadoop Distributed File System)
   2. Larger meaning = HDFS + scheduler (YARN/Mesos/StandAlone) + computing engine (MapReduce or Spark)
- more on architecture: https://github.com/rutgers-oarc/hadoop_intro/blob/master/hadoop-for-everyone.pdf 

## Hadoop history

- Google BigTable - closed source, paper in 2003
- reimplemented by Doug Cutting et al, 2006, at Yahoo
- written in Java
- **IDEA**: use commodity hardware to process large datasets - must guard against hardware failures
- (show Wiki table) - https://en.wikipedia.org/wiki/Apache_Hadoop 
- Apache Foundation: http://www.apache.org/index.html#projects-list  

## HDFS

- each file split in "blocks" (64Mb-128Mb), copied in triplicates (one on the same rack, one on a different rack)
- requires a Node manager - a server that keeps track of where each block is written; as soon as corruption is detected, self-healing starts
- blocks are written on LOCAL hard disks (this is NOT how HPC systems behave - they have dedicated storage separate from compute)
- commands are similar to the usual Unix file system e.g. `hdfs dfs -ls`
- (show demo) - ssh kp807@data1.cs.rutgers.edu
- https://github.com/rutgers-oarc/hadoop_intro/blob/master/hadoop-for-everyone.pdf  - near the end

## MapReduce

- computation occurs in parallel, on each block (Map part)
- then each mapped part gets "reduced" - and possibly shuffled (exchanged between different nodes) in the process

## Example: Word count

- Map phase: each line is split into words, and word is mapped to (word, 1)
- Reduce phase: each tuple is sent to a different node, but same word sent to the same node; then all the multiplicities 
- Example: 
```
this is map    --->  this,is,map  ----> (this,1), (is,1), (map,1)
that is reduce --->  that,is,reduce  ----> (that,1), (is,1), (reduce,1)
```

## Example: group by statement

people = name, age, dept, salary
```
SELECT dept, avg(salary) FROM people GROUP BY dept
```

Map:  
Reduce: 

## Example: Word Count in Hadoop

(go to https://dzone.com/articles/word-count-hello-word-program-in-mapreduce)

## Example: Word Count in Spark

```
lines = spark.read.text("/home/kris/datasets/text/jane_austen.txt").rdd.map(lambda r: r[0])

counts = lines.flatMap(lambda x: x.split(' ')) \
              .map(lambda x: (x, 1)) \
              .reduceByKey(add)
```
(show demo)

## MapReduce shortcomings

- MapReduce was difficult to code in Java (e.g. Yahoo came up with Pig)
- it was not interactive - relatively slow to develop
- quite inefficient - lots of reads and writes


## Example - Word Count in Pig

```
lines = LOAD '/user/hadoop/HDFS_File.txt' AS (line:chararray);
words = FOREACH lines GENERATE FLATTEN(TOKENIZE(line)) as word;
grouped = GROUP words BY word;
wordcount = FOREACH grouped GENERATE group, COUNT(words);
DUMP wordcount;
```

## Hadoop ecosystem

Perennials: 
- HDFS - distributed filesystem
- sqoop - getting data from one system to another (e.g. from database to HDFS)
- Hive = very slow database - but still holdovers
- ZooKeeper = configurations

largely superceded: 
- MapReduce, Pig, Tez = computation engine(s)
- Oozie = workflow manager
- Mahout = machine learning on MapReduce
- Storm = streaming 

New Wave: 
- Apache Spark 
- Kafka
- (containerization; microservices)
- object stores (e.g. S3)

## Key Takeaways from today

- in about 10 years, there was enormous progress in how to store and process big data cheaply
- 30+ components of the Hadoop ecosystem, many of them superceded already
- MapReduce is really split-apply-combine 
- MapReduce has been largely replaced by Apache Spark

## Homework

- install Spark on your laptops - possibly sandbox VM - 
- ssh data1.cs.rutgers.edu  and play around with the hdfs commands - put your favorite dataset in HDFS

## References

install Spark on Windows: https://medium.com/@GalarnykMichael/install-spark-on-windows-pyspark-4498a5d8d66c 
https://community.cloud.databricks.com/  = spark cloud
https://sparkhub.databricks.com/
https://forums.databricks.com/    OR   https://stackoverflow.com
https://spark-packages.org/



## Last time: 

- basics of `hdfs dfs` command
- architecture of HDFS: master/slave (namenode/datanodes)
- compared WordCount in: MapReduce, Pig, and PySpark
- saw pyspark in the shell 
- talked about RDD = resilient distributed dataset = like a list, but partitioned into chunks (partitions), and distributed over several machines

## Today: 

- where to find documentation
- using Spark UI
- using yarn cluster
- Jupyter PySpark: go to https://jupyter.cs.rutgers.edu/ and open new jupyter notebook
- basics of SparkSQL

## References

- Basic guides and documentation: http://spark.apache.org/docs/latest/
- python API docs: http://spark.apache.org/docs/latest/api/python/index.html
- if you want to open pyspark in jupyter notebook on your laptop, add these to your .bashrc file: 
```
export PYSPARK_DRIVER_PYTHON=jupyter
export PYSPARK_DRIVER_PYTHON_OPTS='notebook'
```
(from https://blog.sicara.com/get-started-pyspark-jupyter-guide-tutorial-ae2fe84f594f)

## RDD, functional programming; similar yet different

- map, filter, reduce - exist both in Python and PySpark
- transformations (don't trigger execution) vs actions (trigger execution)

## Try it yourself - exercises

- pick a text file on your laptop 
- load it to spark as an RDD 
- count words starting with a capital letter
- find 5 transformations and 5 actions on RDDs (either from the doc site or by trying it out)

## DataFrame class

- this is like a table
- columnar representation of data
- `select`, `filter`, `sort`, `groupBy`, `as`, `withColumn`, `withColumnRenamed`
- can load/save from/to csv, parquet, json, orc formats - many other formats - also jdbc connector and many other connectors
- can execute sql statements directly
- can define schema programmatically

# Homework

- get a dataset - either from Kaggle or from https://www.ibm.com/communities/analytics/watson-analytics-blog/guide-to-sample-datasets/ 
- repeat the commands from this class to analyze the dataset
- learn how to use window functions 
- choose two large datasets and perform: join using Spark and join in pandas. Benchmark both and compare. 



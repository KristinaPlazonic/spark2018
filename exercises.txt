
# Some exercises

## Where to find information about Spark

The best places to find information about how to use Spark are: 
- [Databricks documentation](https://docs.databricks.com/index.html)
- [Official Spark guide by version](http://spark.apache.org/docs/latest/)
- books: 
  - [Spark: The Definitive Guide, by Matei Zaharia and Bill Chambers](http://shop.oreilly.com/product/0636920034957.do), and its [github code repo](https://github.com/databricks/Spark-The-Definitive-Guide)
  - [High Performance Spark: Best Practices for Scaling and Optimizing Apache Spark, by Holden Karau](https://www.amazon.com/High-Performance-Spark-Practices-Optimizing/dp/1491943203/)


## Basics of Hadoop/HDFS

- How do you put a file from local system to hdfs? 
- How do you get a file from hdfs to the local filesystem? 
- Name 5 HDFS commands that are similar to linux file commands. 
- What is parquet format? If you save a csv file in parquet format, how much compression can you expect? How about loading the data? 
- Do you have to keep a file in parquet format in hdfs? 
- Try loading a dataframe and saving it in HDFS as csv. Will you get one file, or a directory? 
- What is YARN? 
- What is Hive? 

## Basics of Spark

- What is the difference between a transformation and an action? 
- What is Spark GUI? What are the tabs in Spark GUI? 
- What is the difference between RDD and a DataFrame? 
- What is MapReduce? 
- What is WordCount, and how does it work? Can you write down the code for WordCount? 

## Basics of SQL

- How can you define the schema of a DataFrame? 
- Suppose you have a csv file. Write code that will: 
  1. load it in Spark as a DataFrame and let it infer the schema. 
  2. Print the schema. 
  3. Find the maximum and the average of a numerical column. 
  4. Print the counts (frequencies) of a categorical column. 
  5. Suppose the rows represent people, with columns "name", "department", "salary", "city". Write code that will calculate the average salary by department in NYC: 
     - programmatically 
     - by registering a tempView and executing a SQL command
  6. Suppose you have a dataframe where each row is an actor, with columns "name", "movie", "year", and another one with columns "movie", "director". Find all actor-director relationships and how many times they are repeated in the dataset. 

## Writing a batch job

Write your own pyspark app that 
  1. loads two csv files
  2. performs the operation in #6 of the SQL basics above (actors_movies and movies_directors)
  3. writes the result as a parquet file with 10 parts. 



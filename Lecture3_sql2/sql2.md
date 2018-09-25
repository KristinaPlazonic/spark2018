
## Last time: 

- saw RDDs - original Spark data structures - like "chunked lists" - tasks are executed in parallel
- map, filter, flatMap operations on RDDs
- transformations vs actions and lazy evaluations (DAG in SparkUI)
- map, flatMap etc are transformations; take, count, show, write are actions and trigger the actual computation
- examples of DataFrame class = Spark for "table" 
- class Column 
- basic DataFrame operations = show(), select(), groupBy(), count(), filter()
- input a DataFrame from a csv file
- you can register a DataFrame as a temporary table 
- you can use regular SQL expressions on registered tables

## Today: 

- continue on joins
- see more complex data load
- defining your own schema
- do more sql
- get some practice - do your own analysis

## Joins - expensive!

Types of joins: 
- shuffle hash join - When the size of both datasets is large
  - compute the hash value of the columns in the join expression of each row in each dataset and then shuffle those rows with the same hash value to the same partition (by computing modulo)
- broadcast join  - When the size of one of the datasets is small enough to fit into the memory of the executors
  - broadcast a copy of the entire smaller dataset to each of the partitions of the larger dataset i.e. shuffle only the smaller dataset


## Accessing columns

- Column is a class (so is Row)
- there is a function, `col`, in pyspark.sql.functions - after you import that function, you can invoke `col("AIRLINE")`
- in pyspark, there is a pythonic way to invoke the Column, e.g. `flights.AIRLINE`  and `flights['AIRLINE']`

i.e. these three are equivalent
```
>>> fl['YEAR']
Column<b'YEAR'>
>>> fl.YEAR
Column<b'YEAR'>
>>> col('YEAR')
Column<b'YEAR'>
>>> 
```

## Practice questions

Load the flights dataset, which is located in the hdfs directory /user/public/flights-delay. 

- How many files are in that directory? 
- Show a few rows of each file to get a feeling for what data is in each file. 
- What is the name of the airline with the code AS? 
- What is the name of the airport with the code ANC? 
- How many flights were there arriving to ANC? 
- Which airlines are flying to ANC and how many times? 
- create a new column called flight_date from YEAR, MONTH, DAY
- create a new column called flight_date_plus from flight_date with 10 days added
- Which airport has the highest percentage of weather delays? 
- Which airline has the highest cancellation rate? 


- get the 

## References

- data source: https://archive.org/download/stackexchange  
- https://szczeles.github.io/Reading-JSON-CSV-and-XML-files-efficiently-in-Apache-Spark/
- https://github.com/databricks/spark-xml
- https://spark-packages.org/ 


## Important concepts

|| concept || description || example ||
|-----------|-------------|------------------|
| pyspark sql packages | `functions`, `types`, `window`   | `from pyspark.sql import functions` |
| multiline python command |  use either () or \ as line continuation character | |


### Build SparkSession (for within the app)


Every pyspark app will have a SparkSession object. 
```
from pyspark.sql import SparkSession
spark = SparkSession \     
	.builder \     
	.appName("pyspark-example-helloworld") \     
	.config("spark.some.config.option", "some-value") \     
	.getOrCreate()
### your code here
spark.stop()  
```

### Creating a schema for a DataFrame


myschema = df.schema      # from existing DataFrame
myschema = 

### Creating DataFrame objects

df = spark.read.json("customer.json")                 # from json 
df = spark.read.csv("customer.csv")                   # from csv
df = spark.read.option("header", "true").option("inferSchema", "true").csv("customer.csv")    # from csv
df = spark.read.parquet("customer.parquet")    # from parquet
df = pyspark.sql.DataFrame

### Writing DataFrame objects

df.write.csv("my/file/path/data.csv")
df.write.json("my/file/path/data.json")
df.write.parquet("my/file/path/data.parquet")

### Basic dataframe commands

df.dtypes                   #Return df column names and data types
df.show()                   #Display the content of df  - exists in Scala and Java as well. 
df.show(5, False)           #Display first 5 rows of df and do not abbreviate the text within columns
df.head()                   #Return first n rows - pythonic funciton only



Duplicate Values
>>> df = df.dropDuplicates()
Queries
>>> from pyspark.sql import functions as F
Select

>>> df.select("firstName").show()
>>> df.select("firstName","lastName") \
	.show()
>>> df.select("firstName",
		"age",
		explode("phoneNumber") \
	.alias("contactInfo")) \
	.select("contactInfo.type",
		"firstName",  "age") \
>>> df.select(df['age'] > 24).show()
When

>>> df.select("firstName",
		F.when(df.age > 30, 1) \
	.otherwise(0)) \
	.show()
>>> df[df.firstName.isin("Jane","Boris")]
	.collect()
Like

>> df.select("firstName",  Show firstName,
		df.lastName.like("Smith")) \
Startswith – Endswith

>>> df.select("firstName",
		df.lastName \
	.startswith("Sm")) \
>>> df.select(df.lastName.endswith("th"))\
	.show()
Substring

>>> df.select(df.firstName.substr(1, 3) \
	.alias("name")) \
	.collect()
Between

>>> df.select(df.age.between(22, 24)) \
	.show()
Add, Update & Remove Columns
Adding Columns

>>> df = df.withColumn('city',df.address.city) \
	.withColumn('postalCode',df.address.postalCode) \
	.withColumn('state',df.address.state) \
	.withColumn('streetAddress',df.address.streetAddress) \
	.withColumn('telePhoneNumber',
explode(df.phoneNumber.number)) \
	.withColumn('telePhoneType',
		explode(df.phoneNumber.type))
Updating Columns

>> df = df.withColumnRenamed('telePhoneNumber', 'phoneNumber')
Removing Columns

>>> df = df.drop("address", "phoneNumber")
>>> df = df.drop(df.address).drop(df.phoneNumber)
Inspect Data
>>> df.dtypes
Return df column names and data types
>>> df.show()
Display the content of df
>>> df.head()
Return first n rows
>>> df.first()
Return first row
>>> df.take(2)
Return the first n rows
>>> df.schema
Return the schema of df
>>> df.describe().show()
Compute summary statistics
>>> df.columns
Return the columns of df
>>> df.count()
Count the number of rows in df
>>> df.distinct().count()
Count the number of distinct rows in df
>>> df.printSchema()
Print the schema of df
>>> df.explain()
Print the (logical and physical) plans
GroupBy
>>> df.groupBy("age")\

	.count() \

	.show()
Group by age, count the members in the groups
Filter
>>> df.filter(df["age"]>24).show()
Filter entries of age, only keep those
records of which the values are >24
Sort
>>> peopledf.sort(peopledf.age.desc()).collect()
>>> df.sort("age", ascending=False).collect()
>>> df.orderBy(["age","city"],ascending=[0,1])\
	.collect()
Missing & Replacing Values
>>> df.na.fill(50).show()
Replace null values
>>> df.na.drop().show()
Return new df omitting rows with null values
>>> df.na \

	.replace(10, 20) \ 

	.show()
Return new df replacing one value with another
Repartitioning
>>> df.repartition(10)\

	.rdd \

	.getNumPartitions()
df with 10 partitions
>>> df.coalesce(1).rdd.getNumPartitions()
df with 1 partition
Running SQL Queries Programmatically
Registering DataFrames as Views

>>> peopledf.createGlobalTempView("people")
>>> df.createTempView("customer")
>>> df.createOrReplaceTempView("customer")
QueryViews

>>> df5 = spark.sql("SELECT * FROM customer").show()
>>> peopledf2 = spark.sql("SELECT * FROM global_temp.people")\
	.show()
Output
DataStructures

>>> rdd1 = df.rdd               #Convert df into an RDD
>>> df.toJSON().first()         #Convert df into a RDD of string
>>> df.toPandas()               #Return the contents of df as Pandas

DataFrame
Write & Save to Files

>>> df.select("firstName", "city")\
	.write \
	.save("nameAndCity.parquet")
>>> df.select("firstName", "age") \
	.write \
	.save("namesAndAges.json",format="json")
Stopping SparkSession
>>> spark.stop()

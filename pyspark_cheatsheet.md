## Important concepts

| concept | description | example |
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


```
myschema = df.schema      # from existing DataFrame
myschema = 
```

### Creating DataFrame objects

```
df = spark.read.json("customer.json")                                  # from json 
df = spark.read.json("customer.json", schema=myschema)                 # from json 
df = spark.read.csv("customer.csv")                                    # from csv
df = spark.read.option("header", "true").option("inferSchema", "true").csv("customer.csv")    # from csv
df = spark.read.parquet("customer.parquet")                            # from parquet
# TODO: add jdbc

### Writing DataFrame objects

df.write.csv("my/file/path/data.csv")
df.write.json("my/file/path/data.json")
df.write.parquet("my/file/path/data.parquet")
```

### Basic dataframe commands

```
df.dtypes                   #Return df column names and data types
df.show()                   #Display the content of df  - exists in Scala and Java as well. 
df.show(5, False)           #Display first 5 rows of df and do not abbreviate the text within columns
df.head()                   #Return first n rows - pythonic funciton only
df = df.dropDuplicates()    #Duplicate Values
```


####Select
```
from pyspark.sql import functions as F

df.select("firstName").show()
df.select("firstName","lastName").show()
df.select("firstName", "age", explode("phoneNumber") \
	.alias("contactInfo")) \
	.select("contactInfo.type","firstName",  "age") \
df.select(df['age'] > 24).show()
df.select("firstName", F.when(df.age > 30, 1).otherwise(0)).show() #when
df[df.firstName.isin("Jane","Boris")].collect()                    #collect
df.select("firstName",  Show firstName,	df.lastName.like("Smith")) #Like
df.select("firstName", df.lastName  .startswith("Sm")).show()   #Startswith – Endswith
df.select(df.lastName.endswith("th")).show()                    #Substring
df.select(df.firstName.substr(1, 3).alias("name")).collect()
df.select(df.age.between(22, 24)).show()
```

#### Column manipulation - Add, Update & Remove Columns

Adding Columns
```
>>> df = df.withColumn('city',df.address.city) \
	.withColumn('postalCode',df.address.postalCode) \
	.withColumn('state',df.address.state) \
	.withColumn('streetAddress',df.address.streetAddress) \
	.withColumn('telePhoneNumber',
explode(df.phoneNumber.number)) \
	.withColumn('telePhoneType',
		explode(df.phoneNumber.type))
```
```
df = df.withColumnRenamed('telePhoneNumber', 'phoneNumber')   ## Updating Columns
### Removing Columns
df = df.drop("address", "phoneNumber")
df = df.drop(df.address).drop(df.phoneNumber)
df.dtypes
df.printSchema()
df.explain()

df.describe().show()
df.count()
df.distinct().count()
df.columns
```

## GroupBy
```
df.groupBy("age").count().show()
df.filter(df["age"]>24).show()    #Filter entries of age, only keep those records of which the values are >24

peopledf.sort(peopledf.age.desc()).collect()   #sort
df.sort("age", ascending=False).collect()      #sort
df.orderBy(["age","city"],ascending=[0,1]).collect()
df.na.fill(50).show()                         Missing & Replacing Values
df.na.drop().show()                           #Replace null values
df.na.replace(10, 20).show()                  #Return new df omitting rows with null values
df.repartition(10).rdd.getNumPartitions()     #df with 10 partitions
df.coalesce(1).rdd.getNumPartitions()         #df with 1 partition
```
#### QueryViews
```
peopledf.createGlobalTempView("people")
df.createTempView("customer")
df.createOrReplaceTempView("customer")

df5 = spark.sql("SELECT * FROM customer").show()
peopledf2 = spark.sql("SELECT * FROM global_temp.people").show()
```

### Output DataStructures
```
rdd1 = df.rdd               #Convert df into an RDD
df.toJSON().first()         #Convert df into a RDD of string
df.toPandas()               #Return the contents of df as Pandas
```

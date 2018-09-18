CREATE EXTERNAL TABLE iris(
   sepal_length float, sepal_width float,petal_length float,petal_width float, species string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/kp807/smalldata/iris.csv'; 


CREATE TABLE iris(
   sepal_length float, sepal_width float,petal_length float,petal_width float, species string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','; 

LOAD DATA INPATH 'hdfs:/user/kp807/smalldata/iris.csv' INTO TABLE iris;
LOAD DATA INPATH '/user/kp807/smalldata/iris.csv' INTO TABLE iris;


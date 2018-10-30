

https://spark.apache.org/docs/latest/graphx-programming-guide.html#summary-list-of-operators  = 

import org.apache.spark.graphx.GraphLoader

// Load my user data and parse into tuples of user id and attribute list
val SPARKDIR = "/home/kris/spark/spark-2.3.0-bin-hadoop2.7/"
val users = (sc.textFile(s"$SPARKDIR/data/graphx/users.txt").map(line => line.split(",")).map( parts => (parts.head.toLong, parts.tail) ))
val followerGraph = GraphLoader.edgeListFile(sc, s"$SPARKDIR/data/graphx/followers.txt")



import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import org.graphframes._

val filename = "/home/kris/rutgers/teaching/spark/spark_module/Lecture5_graphx/201809-fordgobike-tripdata2.csv"
val vf1 = spark.read.option("header", "true").option("inferSchema", "true").csv(filename).selectExpr("start_station_id AS id", "start_station_name AS station_name")
val vf2 = spark.read.option("header", "true").option("inferSchema", "true").csv(filename).selectExpr("end_station_id AS id", "end_station_name AS station_name")
val vf = (vf1.union(vf2)).distinct()

val ef = spark.read.option("header", "true").option("inferSchema", "true").csv(filename).selectExpr("start_station_id AS src", "end_station_id AS dst", "duration_sec")

val gf = GraphFrame(vf, ef)
val results = gf.pageRank.maxIter(20).run()
results.vertices.sort(desc("pagerank")).show(false)

val motifs: GraphFrame = gf.find("(a)-[e]->(b); (b)-[e2]->(a)")
motifs.show()

sc.setCheckpointDir("/tmp")
val cc = gf.connectedComponents.run()
cc.groupBy("component").count().show()


gf.edges.select("src", "dst").distinct().count()

/////////////////////////////////////////////////////////////////////////


// Examples of Pregel




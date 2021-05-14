// Databricks notebook source
// MAGIC %md
// MAGIC 
// MAGIC ## Overview
// MAGIC 
// MAGIC This notebook will show you how to create and query a table or DataFrame that you uploaded to DBFS. [DBFS](https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html) is a Databricks File System that allows you to store data for querying inside of Databricks. This notebook assumes that you have a file already inside of DBFS that you would like to read from.
// MAGIC 
// MAGIC This notebook is written in **Python** so the default cell type is Python. However, you can use different languages by using the `%LANGUAGE` syntax. Python, Scala, SQL, and R are all supported.

// COMMAND ----------

// MAGIC %scala
// MAGIC //File location and type
// MAGIC val file_location = "/FileStore/tables/soc_LiveJournal1Adj.txt"
// MAGIC val file_type = "txt"
// MAGIC 
// MAGIC //CSV options
// MAGIC val infer_schema = "false"
// MAGIC val first_row_is_header = "false"
// MAGIC val delimiter = ","
// MAGIC 
// MAGIC //The applied options are for CSV files. For other file types, these will be ignored.
// MAGIC val df = spark.read.option("sep", delimiter) .csv(file_location)
// MAGIC 
// MAGIC 
// MAGIC df.show(10)

// COMMAND ----------

//Q1 start

val lines = sc.textFile("/FileStore/tables/soc_LiveJournal1Adj.txt")
//lines.take(10).foreach(println)

// COMMAND ----------

def intersection(first: Set[String], second: Set[String]) = {
    first.toSet intersect second.toSet
  }

def Map(line: String) = {
    val line1 = line.split("\\t+")
    val person = line1(0)
    val newfriends = if (line1.length > 1) line1(1) else "null"
    val nfriends = newfriends.split(",")
    val friends = for (i <- 0 to nfriends.length - 1) yield nfriends(i)
    val pairs = nfriends.map(friend => {
      if (person < friend) person + "," + friend else friend + "," + person
    })
    pairs.map(pair => (pair, friends.toSet))
  
      
  }
val result = lines.flatMap(Map)
      .reduceByKey(intersection)
      .filter(!_._2.equals("null")).filter(!_._2.isEmpty)
      .sortByKey()

val res = result.toDF("ID","friends").registerTempTable("Q1")
val outputQ1 = spark.sql("SELECT ID,size(friends) as count from Q1")

//Q1 output
//res.withColumn("number_of" ,size($"friends")).show
//result.take(10)
//res.select(size($"friends"))
outputQ1.show()


// COMMAND ----------

//Q2 start
val Q2 = spark.sql("SELECT max(size(friends)) as count from Q1")
//Q2.show()
val Q2res = spark.sql("SELECT ID,size(friends) as count from Q1 where size(friends) = 99")
//Q2 output
Q2res.show()

// COMMAND ----------



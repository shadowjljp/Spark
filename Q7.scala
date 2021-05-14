// Databricks notebook source
// MAGIC %md
// MAGIC 
// MAGIC ## Overview
// MAGIC 
// MAGIC This notebook will show you how to create and query a table or DataFrame that you uploaded to DBFS. [DBFS](https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html) is a Databricks File System that allows you to store data for querying inside of Databricks. This notebook assumes that you have a file already inside of DBFS that you would like to read from.
// MAGIC 
// MAGIC This notebook is written in **Python** so the default cell type is Python. However, you can use different languages by using the `%LANGUAGE` syntax. Python, Scala, SQL, and R are all supported.

// COMMAND ----------




// The applied options are for CSV files. For other file types, these will be ignored.
val lines = sc.textFile("/FileStore/tables/userdata.txt")
lines.take(10).foreach(println)

// COMMAND ----------

//var count=0
//THIS
def union(first: List[Long], second: List[Long]) = {
    val list = first ++ second
    val sortedList =list.sorted
  sortedList
 }
val result = lines.zipWithIndex().flatMap({ 
  case (line, lineNumber) => {
    var words = line.split(",")
    var pairs = words.map(word =>{
      (word.toString,List(lineNumber+1))
    })
    pairs
                             }

}).reduceByKey(union)
result.collect().foreach(println)

// COMMAND ----------

import java.io._ 
def union(first: List[Long], second: List[Long]) = {
    val list = first ++ second
    val sortedList =list.sorted
  sortedList
 }
val result = lines.zipWithIndex().flatMap({ 
  case (line, lineNumber) => {
    var words = line.split(",")
    var pairs = words.map(word =>{
      (word.toString,List(lineNumber+1))
    })
    pairs
                             }

}).reduceByKey(union)

def writeFile(filename: String, lines: Array([String,List[String])): Unit = {
    val file = new File(filename)
    val bw = new BufferedWriter(new FileWriter(file))
    for (line <- lines) {
        bw.write(line)
    }
    bw.close()
}
//val seqResult = result.collect().toSeq
//writeFile("Q7",seqResult)
writeFile("Q7",result)

// COMMAND ----------

import java.io._ 
def union(first: List[Long], second: List[Long]) = {
    val list = first ++ second
    val sortedList =list.sorted
  sortedList
 }
val result = lines.zipWithIndex().flatMap({ 
  case (line, lineNumber) => {
    var words = line.split(",")
    var pairs = words.map(word =>{
      (word.toString,List(lineNumber+1))
    })
    pairs
                             }

}).reduceByKey(union)

// def writeFile(filename: String, lines: Array[(String, List[Long])]): Unit = {
//     val file = new File(filename)
//     val bw = new BufferedWriter(new FileWriter(file))
//     for (line <- lines) {
//         bw.write(line)
//     }
//     bw.close()
// }
def writeFile(filename: String, s: String): Unit = {
    val file = new File(filename)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(s)
    bw.close()
}

var re = result.collect().mkString("\n")
//writeFile("Q7output",re)
dbutils.fs.put("/FileStore/my-stuff/Q7output.txt",re)

// COMMAND ----------



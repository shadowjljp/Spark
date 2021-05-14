// Databricks notebook source
// MAGIC %python
// MAGIC # File location and type
// MAGIC file_location = "/FileStore/tables/review.csv"
// MAGIC file_type = "csv"
// MAGIC 
// MAGIC # CSV options
// MAGIC infer_schema = "false"
// MAGIC first_row_is_header = "false"
// MAGIC delimiter = "::"
// MAGIC 
// MAGIC # The applied options are for CSV files. For other file types, these will be ignored.
// MAGIC df = spark.read.format(file_type) \
// MAGIC   .option("inferSchema", infer_schema) \
// MAGIC   .option("header", first_row_is_header) \
// MAGIC   .option("sep", delimiter) \
// MAGIC   .load(file_location).toDF("Review_id",'User_id','Business_id',"Stars")
// MAGIC 
// MAGIC display(df)

// COMMAND ----------

// MAGIC %python
// MAGIC # Create a view or table
// MAGIC 
// MAGIC temp_table_name = "review"
// MAGIC 
// MAGIC df.createOrReplaceTempView(temp_table_name)

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC /* Query the created temp table in a SQL cell */
// MAGIC 
// MAGIC select * from `review`

// COMMAND ----------

// MAGIC %python
// MAGIC # File location and type
// MAGIC file_location = "/FileStore/tables/business.csv"
// MAGIC file_type = "csv"
// MAGIC 
// MAGIC # CSV options
// MAGIC infer_schema = "false"
// MAGIC first_row_is_header = "false"
// MAGIC delimiter = "::"
// MAGIC 
// MAGIC # The applied options are for CSV files. For other file types, these will be ignored.
// MAGIC df = spark.read.format(file_type) \
// MAGIC   .option("inferSchema", infer_schema) \
// MAGIC   .option("header", first_row_is_header) \
// MAGIC   .option("sep", delimiter) \
// MAGIC   .load(file_location).toDF("Business_id",'Full_address','Categories')
// MAGIC 
// MAGIC display(df)

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC temp_table_name = "business"
// MAGIC 
// MAGIC df.createOrReplaceTempView(temp_table_name)

// COMMAND ----------

// MAGIC %python
// MAGIC # With this registered as a temp view, it will only be available to this particular notebook. If you'd like other users to be able to query this table, you can also create a table from the DataFrame.
// MAGIC # Once saved, this table will persist across cluster restarts as well as allow various users across different notebooks to query this data.
// MAGIC # To do so, choose your table name and uncomment the bottom line.
// MAGIC 
// MAGIC permanent_table_name = "review_csv"
// MAGIC 
// MAGIC # df.write.format("parquet").saveAsTable(permanent_table_name)

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from `business`

// COMMAND ----------

// MAGIC %scala
// MAGIC val Result = spark.sql("SELECT DISTINCT r.User_id, r.Stars as Rating, b.Business_id " +
// MAGIC       "from review as r INNER JOIN business as b ON r.Business_id = b.Business_id " +
// MAGIC       "where b.Full_address like '%Stanford%'  ").select("User_id", "Rating")
// MAGIC //Q3 output
// MAGIC Result.show()

// COMMAND ----------

//Result.collect().map(_.getString(0)).mkString("\n")

// COMMAND ----------

//dbutils.fs.put("/FileStore/my-stuff/Q3output.txt",Result.collect().map(_.getString(0)).mkString("\n"))

// COMMAND ----------

// MAGIC %scala
// MAGIC //Q4
// MAGIC val Result = spark.sql("SELECT  AVG(r.Stars) as avg_rating, b.Business_id, b.Full_address, b.Categories " +
// MAGIC       "from review as r INNER JOIN business as b ON r.Business_id = b.Business_id " +
// MAGIC       " group by  b.Business_id, b.Full_address, b.Categories order by avg_rating desc limit 10 ").select("Business_id","Full_address","Categories", "avg_rating")
// MAGIC //Q4 output
// MAGIC Result.show()
// MAGIC //dbutils.fs.put("/FileStore/my-stuff/Q4output.txt",Result)

// COMMAND ----------

//Q5
val Result = spark.sql("SELECT   count(b.Business_id) as Count, b.Categories " +
      "from review as r INNER JOIN business as b ON r.Business_id = b.Business_id " +
      " group by b.Categories ").select("Categories", "Count")
//Q5 output
Result.show()
//dbutils.fs.put("/FileStore/my-stuff/Q5output.txt",Result)

// COMMAND ----------

//Q6
val Result = spark.sql("SELECT   count(b.Business_id) as Count, b.Categories " +
      "from review as r INNER JOIN business as b ON r.Business_id = b.Business_id " +
      " group by b.Categories order by Count desc limit 10 ").select("Categories", "Count")
//Q6 output
Result.show()
//dbutils.fs.put("/FileStore/my-stuff/Q6output.txt",Result)

// COMMAND ----------



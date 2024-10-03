-- Databricks notebook source
-- MAGIC %fs ls "dbfs:/databricks-datasets/learning-spark-v2/flights/summary-data/json/"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC ##Creating a checkpoint location 
-- MAGIC checkpoint_location = "dbfs:/user/hive/flights_summary/checkpoint"
-- MAGIC dbutils.fs.rm(checkpoint_location, True)
-- MAGIC dbutils.fs.mkdirs(checkpoint_location)
-- MAGIC
-- MAGIC ##Creating a schema location 
-- MAGIC schema_location = "dbfs:/user/hive/flights_summary/schema"
-- MAGIC dbutils.fs.rm(schema_location, True)
-- MAGIC dbutils.fs.mkdirs(schema_location)
-- MAGIC
-- MAGIC ##defining source location of the table
-- MAGIC source_location = "dbfs:/user/hive/flights_summary/source"
-- MAGIC dbutils.fs.rm(source_location, True)
-- MAGIC dbutils.fs.mkdirs(source_location)
-- MAGIC
-- MAGIC ##Copy first dataset file
-- MAGIC dbutils.fs.cp("dbfs:/databricks-datasets/learning-spark-v2/flights/summary-data/json/2010-summary.json",source_location)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls(source_location)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.readStream\
-- MAGIC .format("cloudFiles")\
-- MAGIC .option("cloudFiles.format", "json")\
-- MAGIC .option("cloudFiles.schemaLocation", schema_location)\
-- MAGIC .load(source_location)\
-- MAGIC .createOrReplaceTempView("vw_flights_summary")

-- COMMAND ----------

select count(*) from vw_flights_summary;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC ##Copy more data into the source cloud files
-- MAGIC dbutils.fs.cp("dbfs:/databricks-datasets/learning-spark-v2/flights/summary-data/json/2012-summary.json",source_location)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #Write view data to a persistant table
-- MAGIC spark.table("vw_flights_summary").writeStream\
-- MAGIC .option("checkpointLocation", checkpoint_location)\
-- MAGIC .option("mergeSchema", "true")\
-- MAGIC .table("tbl_flights_summary_Bronze")

-- COMMAND ----------

SELECT * FROM tbl_flights_summary_Bronze;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC ##run the following cell to stop all active streaming queries.
-- MAGIC for s in spark.streams.active:
-- MAGIC     print("Stopping " + s.id)
-- MAGIC     s.stop()
-- MAGIC     s.awaitTermination()

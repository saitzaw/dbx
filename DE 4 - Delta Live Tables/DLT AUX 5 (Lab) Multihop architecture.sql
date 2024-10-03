-- Databricks notebook source
-- MAGIC %md
-- MAGIC ####Using a sample Databricks dataset to demonstrate Multihop architecture
-- MAGIC

-- COMMAND ----------

-- MAGIC %fs ls "dbfs:/databricks-datasets/learning-spark-v2/flights/summary-data/json/"

-- COMMAND ----------

SELECT * FROM json.`dbfs:/databricks-datasets/learning-spark-v2/flights/summary-data/json/2010-summary.json`

-- COMMAND ----------

-- MAGIC %python
-- MAGIC ##defining source location of the table
-- MAGIC source_location = "dbfs:/user/hive/flights_summary/source"
-- MAGIC dbutils.fs.rm(source_location, True)
-- MAGIC dbutils.fs.mkdirs(source_location)
-- MAGIC ##Copy first dataset file
-- MAGIC dbutils.fs.cp("dbfs:/databricks-datasets/learning-spark-v2/flights/summary-data/json/2010-summary.json",source_location)
-- MAGIC
-- MAGIC ##Creating a schema bronze location 
-- MAGIC schema_location_bronze = "dbfs:/user/hive/flights_summary/bronze/schema"
-- MAGIC dbutils.fs.rm(schema_location_bronze, True)
-- MAGIC dbutils.fs.mkdirs(schema_location_bronze)
-- MAGIC
-- MAGIC ##Creating a checkpoint Bronze location 
-- MAGIC checkpoint_location_bronze = "dbfs:/user/hive/flights_summary/bronze/checkpoint"
-- MAGIC dbutils.fs.rm(checkpoint_location_bronze, True)
-- MAGIC dbutils.fs.mkdirs(checkpoint_location_bronze)
-- MAGIC
-- MAGIC ##Creating a checkpoint Silver location 
-- MAGIC checkpoint_location_Silver = "dbfs:/user/hive/flights_summary/Silver/checkpoint"
-- MAGIC dbutils.fs.rm(checkpoint_location_Silver, True)
-- MAGIC dbutils.fs.mkdirs(checkpoint_location_Silver)
-- MAGIC
-- MAGIC ##Creating a checkpoint Gold location 
-- MAGIC checkpoint_location_Gold = "dbfs:/user/hive/flights_summary/Gold/checkpoint"
-- MAGIC dbutils.fs.rm(checkpoint_location_Gold, True)
-- MAGIC dbutils.fs.mkdirs(checkpoint_location_Gold)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Using Auto loader to load data from Source to Bronze Layer

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.readStream\
-- MAGIC .format("cloudFiles")\
-- MAGIC .option("cloudFiles.format", "json")\
-- MAGIC .option("cloudFiles.schemaLocation", schema_location_bronze)\
-- MAGIC .load(source_location)\
-- MAGIC .createOrReplaceTempView("vw_flights_summary_source")

-- COMMAND ----------

--Insert auditing columns in Bronze layer
CREATE OR REPLACE TEMPORARY VIEW vw_flights_summary_Bronze
AS
SELECT *, current_timestamp() as inserted_utc_datetime, input_file_name() as input_file_name FROM vw_flights_summary_source;

-- COMMAND ----------

-- check data from the the summary bornze table
SELECT 
  DEST_COUNTRY_NAME
  , ORIGIN_COUNTRY_NAME
  , count
  , inserted_utc_datetime
  , input_file_name  
FROM vw_flights_summary_Bronze;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #Write view data to a persistant table
-- MAGIC spark.table("vw_flights_summary_Bronze").writeStream\
-- MAGIC .option("checkpointLocation", checkpoint_location_bronze)\
-- MAGIC .option("mergeSchema", "true")\
-- MAGIC .outputMode("append")\
-- MAGIC .table("tbl_flights_summary_Bronze")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Using Structured Streaming to load data from Bronze to Silver Layer

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Read data from the bronze layer
-- MAGIC spark.readStream\
-- MAGIC .table("tbl_flights_summary_Bronze")\
-- MAGIC .createOrReplaceTempView("vw_flights_summary_Bronze")

-- COMMAND ----------

--Clean data in the Bronze layer
CREATE OR REPLACE TEMPORARY VIEW vw_flights_summary_cleaned_Bronze
AS
SELECT 
  DEST_COUNTRY_NAME
  , ORIGIN_COUNTRY_NAME
  , count as NumOfFlights
  , inserted_utc_datetime
  , input_file_name  
FROM vw_flights_summary_Bronze WHERE DEST_COUNTRY_NAME = 'United States'

-- COMMAND ----------

SELECT 
  DEST_COUNTRY_NAME
  , ORIGIN_COUNTRY_NAME
  , NumOfFlights
  , inserted_utc_datetime
  , input_file_name 
FROM vw_flights_summary_cleaned_Bronze;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #Write view data to a persistant table
-- MAGIC spark.table("vw_flights_summary_cleaned_Bronze").writeStream\
-- MAGIC .option("checkpointLocation", checkpoint_location_Silver)\
-- MAGIC .outputMode("append")\
-- MAGIC .table("tbl_flights_summary_Silver")
-- MAGIC

-- COMMAND ----------

select * from tbl_flights_summary_Silver

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Load data from Silver to Gold Layer

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.readStream\
-- MAGIC .table("tbl_flights_summary_Silver")\
-- MAGIC .createOrReplaceTempView("vw_flights_summary_Silver")

-- COMMAND ----------

--Apply Business logic (transformations) for the Gold layer
--Clean data in the Bronze layer
CREATE OR REPLACE TEMPORARY VIEW vw_flights_summary_Silver_Aggregated
AS
SELECT 
    'Flights to US' AS DEST_US
    , SUM(NumOfFlights) as TotalFlights
    , inserted_utc_datetime
    , input_file_name 
FROM vw_flights_summary_Silver
GROUP BY inserted_utc_datetime, input_file_name


-- COMMAND ----------

select * from vw_flights_summary_Silver_Aggregated

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #Write view data to a persistant table
-- MAGIC spark.table("vw_flights_summary_Silver_Aggregated").writeStream\
-- MAGIC .option("checkpointLocation", checkpoint_location_Gold)\
-- MAGIC .outputMode("complete")\
-- MAGIC .trigger(availableNow=True)\
-- MAGIC .table("tbl_flights_summary_Gold")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####View the Gold Layer Output Data

-- COMMAND ----------

SELECT * FROM tbl_flights_summary_Gold;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC ##Load more data
-- MAGIC dbutils.fs.cp("dbfs:/databricks-datasets/learning-spark-v2/flights/summary-data/json/2011-summary.json",source_location)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC ##run the following cell to stop all active streaming queries.
-- MAGIC for s in spark.streams.active:
-- MAGIC     print("Stopping " + s.id)
-- MAGIC     s.stop()
-- MAGIC     s.awaitTermination()

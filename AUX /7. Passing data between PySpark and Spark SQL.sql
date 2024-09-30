-- Databricks notebook source
-- MAGIC %md
-- MAGIC ####Passing data between PySpark (Dataframe) and Spark SQL (View)
-- MAGIC ####Convert from Spark SQL table/view to PySpark Dataframe
-- MAGIC DF = spark.sql("SELECT * FROM TABLE/VIEW")
-- MAGIC
-- MAGIC ####Convert from PySpark Dataframe to Spark SQL view
-- MAGIC DF.createOrReplaceTempView("VIEW_NAME")
-- MAGIC
-- MAGIC DF.createTempView("VIEW_NAME")

-- COMMAND ----------

--Creating a table from a databricks sample dataset 'flights'
CREATE OR REPLACE TABLE tbl_flights
AS SELECT * FROM json.`dbfs:/databricks-datasets/learning-spark-v2/flights/summary-data/json/`;

-- COMMAND ----------

SELECT * from tbl_flights limit 10;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #Converting table tbl_flights to a Pyspark dataframe
-- MAGIC
-- MAGIC df_flights = spark.table("tbl_flights")
-- MAGIC df_flights.select('DEST_COUNTRY_NAME', 'ORIGIN_COUNTRY_NAME', 'count').show()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #Converting dataframe df_flights to a SQL view vw_flights using createTempView
-- MAGIC df_flights.createTempView("vw_flights")

-- COMMAND ----------

SELECT * FROM vw_flights;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #Converting Pyspark dataframe tbl_flights to a Pyspark dataframe using createOrReplaceTempView
-- MAGIC df_flights.createOrReplaceTempView("vw_flights")

-- COMMAND ----------

select * from vw_flights;

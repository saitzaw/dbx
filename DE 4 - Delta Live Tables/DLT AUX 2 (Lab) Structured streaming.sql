-- Databricks notebook source
-- MAGIC %md
-- MAGIC Streaming delta table source(Computer.Accessories) -----> Destination table(tbl_Computer_Accessories_Bronze)

-- COMMAND ----------

--CREATING sample Datasets to use as source
DROP DATABASE IF EXISTS Computer CASCADE;
CREATE DATABASE Computer;

-- create table 
CREATE OR REPLACE TABLE Computer.Accessories
(  
    ID INT
    , Part STRING
    , Quantity INT
    , InsertedDateTime TIMESTAMP
);

-- insert data into tablle 
INSERT INTO Computer.Accessories
VALUES 
    (1, 'Mouse', 10, GETDATE() )
    , (2, 'Keyboard', 10, GETDATE())
    , (3, 'Monitor', 4, GETDATE());

-- check insert data
select * from Computer.Accessories

-- COMMAND ----------

-- MAGIC %python
-- MAGIC ##Creating a checkpoint location and defining source location of the table
-- MAGIC checkpoint_location = "dbfs:/user/hive/checkpoint_Computer_Accessories"
-- MAGIC dbutils.fs.rm(checkpoint_location, True)
-- MAGIC dbutils.fs.mkdirs(checkpoint_location)
-- MAGIC source_location = "dbfs:/user/hive/warehouse/computer.db/accessories"

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC # check the location 
-- MAGIC dbutils.fs.ls('dbfs:/user/hive/warehouse/computer.db/accessories')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # READ stream data from the view table
-- MAGIC # read from the streaming data source -> table 
-- MAGIC # create a temporary view table using df.createOrReplaceTempView()
-- MAGIC spark.readStream\
-- MAGIC .table("Computer.Accessories")\
-- MAGIC .createOrReplaceTempView("vw_Computer_Accessories_Bronze")

-- COMMAND ----------

SELECT COUNT(*) FROM vw_Computer_Accessories_Bronze;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.table("vw_Computer_Accessories_Bronze").writeStream\
-- MAGIC .option("checkpointLocation", checkpoint_location)\
-- MAGIC .option("mergeSchema", "true")\
-- MAGIC .table("tbl_Computer_Accessories_Bronze")

-- COMMAND ----------

--Insert data into the streaming table source
INSERT INTO Computer.Accessories
VALUES (1, 'Mouse', 20, GETDATE() ), (2, 'Keyboard', 15, GETDATE()), (3, 'Monitor', 6, GETDATE())

-- COMMAND ----------

SELECT * FROM tbl_Computer_Accessories_Bronze

-- COMMAND ----------

INSERT INTO Computer.Accessories
VALUES (4, 'Mouse', 22, GETDATE() ), (5, 'Keyboard', 15, GETDATE()), (6, 'Monitor', 18, GETDATE())

-- COMMAND ----------

-- MAGIC %python
-- MAGIC ##run the following cell to stop all active streaming queries.
-- MAGIC for s in spark.streams.active:
-- MAGIC     print("Stopping " + s.id)
-- MAGIC     s.stop()
-- MAGIC     s.awaitTermination()

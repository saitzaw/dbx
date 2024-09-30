-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Lesson goals
-- MAGIC
-- MAGIC #### 1. Create Delta table using CTAS (create table as select)
-- MAGIC
-- MAGIC #### 2. Create table from views
-- MAGIC
-- MAGIC #### 3. typecast timestamp to calender date
-- MAGIC
-- MAGIC #### 4. Create columns for automated metadata

-- COMMAND ----------

--CREATING SAMPLE DATASET
DROP DATABASE IF EXISTS Computer CASCADE;
CREATE DATABASE Computer;

--Insert Data
CREATE OR REPLACE TABLE Computer.Accessories
(  ID INT,  Part STRING,  Quantity INT, AuditDateTime LONG);
INSERT INTO Computer.Accessories
VALUES (1, 'Mouse', 10, 1593881096290 ), (2, 'Keyboard', 20, 1593881095799), (3, 'Monitor', 99, 1593881093452), (4, 'Webcam', 22, 1593881093394 ), (5, 'Charger', 10, 1593881092076), (6, 'Connectors', 41, 1593879303631), (7, 'Printer', 56, 1593879304224), (8, 'Scanner', 32, 1593879305465), (9, 'External Drive', 64, 1593879305482), (10, 'Headphones', 48, 1593879305746);

--Show data
select * from Computer.Accessories
--Note: timestamp is recorded as milliseconds in epoch in LONG format

-- COMMAND ----------

--Let ujs check the type of the table. Is it delta by default?
DESCRIBE TABLE EXTENDED Computer.Accessories

-- COMMAND ----------

--typecast timestamp to calender date
CREATE OR REPLACE VIEW Computer.vw_Accessories
AS
SELECT ID, Part, Quantity
, cast(cast(AuditDateTime/1e6 AS TIMESTAMP) AS DATE) AS AuditDate 
FROM Computer.Accessories;

-- COMMAND ----------

SELECT * FROM Computer.vw_Accessories

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <b> Metadata <b>
-- MAGIC
-- MAGIC current_timestamp() records the timestamp when the logic is executed.
-- MAGIC
-- MAGIC input_file_name() records the source data file for each record in the table.
-- MAGIC

-- COMMAND ----------

--Using create table as select and adding metadata
CREATE OR REPLACE TABLE Computer.tbl_Accessories
COMMENT "Contains computer accessories information"
AS
SELECT *
, current_timestamp() as insertedTimestamp
, input_file_name() as sourceFileInfo
FROM Computer.vw_Accessories

-- COMMAND ----------

SELECT * FROM Computer.tbl_Accessories

-- COMMAND ----------

DESCRIBE TABLE EXTENDED Computer.tbl_Accessories

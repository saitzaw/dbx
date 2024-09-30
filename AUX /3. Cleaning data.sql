-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Lesson goals
-- MAGIC
-- MAGIC #### 1. Counting records (all, nulls, distinct and condition based)
-- MAGIC
-- MAGIC #### 2. Distinct records
-- MAGIC
-- MAGIC #### 3. Primary key uniqueness check
-- MAGIC
-- MAGIC #### 4. Search using Regex

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Prepare sample datasets - Computer.Accessories_dirty
-- MAGIC
-- MAGIC This dataset has duplicate records, null record, records with quantity as null and zero

-- COMMAND ----------

--CREATING SAMPLE DATASET
DROP DATABASE IF EXISTS Computer CASCADE;
CREATE DATABASE Computer;

--Insert Data
CREATE OR REPLACE TABLE Computer.Accessories_dirty
(  ID INT,  Part STRING,  Quantity INT, AuditDateTime LONG);
INSERT INTO Computer.Accessories_dirty
VALUES (1, 'Stylus', 10, 1593881096290), (2, 'Pencil', 20, 1593881095799), (3, 'Multi-USB', 99, 1593881093452), (4, 'Monitor - 1', 22, 1593881093394 ), (4, 'Monitor - 1', 22, 1593881093394 ), (5, 'HDMI cable', 10, 1593881092076), (5, 'HDMI cable', 10, 1593881092076), (6, 'USB-A cable', 41, 1593879303631), (7, 'USB-C cable', 56, 1593879304224), (8, 'VGA Adaptor',null , 1593879305465), (9, 'External Drive', 64, 1593879305482), (10, 'Headphones', 0, 1593879305754), (11, 'Mouse', 10, 1593881096290 ), (12, 'Keyboard', 20, 1593881095799), (13, 'Monitor - 2', 99, 1593881093452), (14, 'Webcam', 22, 1593881093394 ), (15, 'Charger', 10, 1593881092076), (9, 'External Drive', 64, 1593879305482), (10, 'Headphones', 48, 1593879305746), (null, null, null, null);

--Show data
select * from Computer.Accessories_dirty
--Note: timestamp is recorded as milliseconds in epoch in LONG format

-- COMMAND ----------

--Examine difference between COUNT(*) and COUNT(column)
--COUNT(*) - counts all the rows
--COUNT(DISTINCT column) - counts distinct column values
--COUNT(column) - skips the null values
--COUNT_IF() - counts based on the provided condition

select COUNT(*) as NetRecords
, COUNT(DISTINCT *) as NumDistinctRecords
, COUNT(DISTINCT Part) as NumDistinctParts
, COUNT(Part) NumNotNULLParts
, COUNT(Quantity) NumNotNULLQty_1
, COUNT_IF(QUANTITY IS NOT NULL) as NumNotNULLQty_2
, COUNT_IF(QUANTITY IS NULL) AS NumNULLQty
, COUNT_IF(QUANTITY > 40) AS NumQTY_Above40
from Computer.Accessories_dirty

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Using data profile

-- COMMAND ----------

select * from Computer.Accessories_dirty

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Distinct records

-- COMMAND ----------

CREATE OR REPLACE TABLE Computer.Accessories_distinct
select DISTINCT(*) from Computer.Accessories_dirty

-- COMMAND ----------

select * from Computer.Accessories_distinct

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Deduplicating records based on a column
-- MAGIC
-- MAGIC We do not want the records with ID or Quantity are NULL, and want to combine the records with same Part (example headphone) by adding quantity and taking the maximum of the AuditDateTime

-- COMMAND ----------

CREATE OR REPLACE TABLE Computer.Accessories_dedupe
select ID, Part, SUM(Quantity) AS Quantity, MAX(AuditDateTime) AuditDateTime
from Computer.Accessories_dirty
WHERE ID IS NOT NULL and Quantity IS NOT NULL
GROUP BY ID, PART

-- COMMAND ----------

--Verify the output
SELECT * FROM Computer.Accessories_dedupe
ORDER BY ID ASC

-- COMMAND ----------

--Let us verify using the query used before DeDuping
select COUNT(*) as NetRecords
, COUNT(DISTINCT *) as NumDistinctRecords
, COUNT(DISTINCT Part) as NumDistinctParts
, COUNT(Part) NumNotNULLParts
, COUNT(Quantity) NumNotNULLQty_1
, COUNT_IF(QUANTITY IS NOT NULL) as NumNotNULLQty_2
, COUNT_IF(QUANTITY IS NULL) AS NumNULLQty
from Computer.Accessories_dedupe

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Verify that primary key is unique

-- COMMAND ----------

--lets assume that Id is a primary key in table Computer.Accessories_dedupe

-- COMMAND ----------

SELECT count(distinct ID) distinct_ID_Count, count(*) Net_Records 
FROM Computer.Accessories_dedupe

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Verify that primary key has unique value association with other field
-- MAGIC
-- MAGIC In our case let us verify relation between ID and Part

-- COMMAND ----------

SELECT ID, count(Part)
FROM Computer.Accessories_dedupe
GROUP BY ID
HAVING count(Part) > 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Search for a specific value in a field
-- MAGIC

-- COMMAND ----------

SELECT * FROM Computer.Accessories_dedupe
WHERE UPPER(Part) like '%MONITOR%'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Using regex

-- COMMAND ----------

--Let us search for word 'cable' in column Part using regex
select *
from Computer.Accessories_dirty AS email_domain
where regexp_extract(Part, "(?i)cable$", 0) = 'cable'

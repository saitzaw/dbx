-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Lesson goals
-- MAGIC
-- MAGIC #### 1. Insert Into
-- MAGIC
-- MAGIC #### 2. Insert Overwrite
-- MAGIC
-- MAGIC #### 3. Merge Into
-- MAGIC
-- MAGIC #### 4. Copy Into

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Prepare sample datasets - Computer.Accessories_1 & Computer.Accessories_2

-- COMMAND ----------

--CREATING SAMPLE DATASET
DROP DATABASE IF EXISTS Computer CASCADE;
CREATE DATABASE Computer;

--Insert Data
CREATE OR REPLACE TABLE Computer.Accessories_1
(  ID INT,  Part STRING,  Quantity INT, AuditDateTime LONG);
INSERT INTO Computer.Accessories_1
VALUES (1, 'Stylus', 10, 1593881096290), (2, 'Pencil', 20, 1593881095799), (3, 'Multi-USB', 99, 1593881093452), (4, 'Monitor - 1', 22, 1593881093394 ), (5, 'HDMI cable', 10, 1593881092076), (6, 'USB-A cable', 41, 1593879303631), (7, 'USB-C cable', 56, 1593879304224), (8, 'VGA Adaptor', 32, 1593879305465), (9, 'External Drive', 64, 1593879305482), (10, 'Headphones', 48, 1593879305746);

--Insert Data
CREATE OR REPLACE TABLE Computer.Accessories_2
(  ID INT,  Part STRING,  Quantity INT, AuditDateTime LONG);
INSERT INTO Computer.Accessories_2
VALUES (11, 'Mouse', 10, 1593881096290 ), (12, 'Keyboard', 20, 1593881095799), (13, 'Monitor - 2', 99, 1593881093452), (14, 'Webcam', 22, 1593881093394 ), (15, 'Charger', 10, 1593881092076), (16, 'Connectors', 41, 1593879303631), (17, 'Printer', 56, 1593879304224), (18, 'Scanner', 32, 1593879305465), (9, 'External Drive', 64, 1593879305482), (10, 'Headphones', 48, 1593879305746);

--Show data
select * from Computer.Accessories_1
--Note: timestamp is recorded as milliseconds in epoch in LONG format

-- COMMAND ----------

--Show data
select * from Computer.Accessories_2

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Use CREATE OR REPLACE TABLE (CRAS) to create and insert data.
-- MAGIC This command will create a new table if not already created.

-- COMMAND ----------

CREATE OR REPLACE TABLE Computer.bronze_Accessories
SELECT *, current_timestamp() as insertedTimestamp FROM Computer.Accessories_1

-- COMMAND ----------

SELECT * FROM Computer.bronze_Accessories --shows 10 records

-- COMMAND ----------

-- MAGIC %md
-- MAGIC APPEND ROWS

-- COMMAND ----------

INSERT INTO Computer.bronze_Accessories
SELECT *, current_timestamp() FROM Computer.Accessories_2

-- COMMAND ----------

SELECT * FROM Computer.bronze_Accessories

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Use INSERT OVERWRITE to truncate and insert data.
-- MAGIC
-- MAGIC This command cannot create a table if not already created.
-- MAGIC
-- MAGIC This command will fail of the input schema does not match with the data being inserted.
-- MAGIC
-- MAGIC This is fast as compared to MERGE operation if we want to simply replace the data with the data being inserted.

-- COMMAND ----------

INSERT OVERWRITE Computer.bronze_Accessories
SELECT *, current_timestamp() FROM Computer.Accessories_1

-- COMMAND ----------

SELECT * FROM Computer.bronze_Accessories --still shows 10 records as the previous records have been dropped before the new records were inserted

-- COMMAND ----------

DESCRIBE HISTORY Computer.bronze_Accessories

-- COMMAND ----------

-- MAGIC %md
-- MAGIC MERGE INTO
-- MAGIC
-- MAGIC *updates, inserts, and deletes are completed as a single transaction. Avoids duplication
-- MAGIC
-- MAGIC *multiple conditionals can be added in addition to matching fields
-- MAGIC
-- MAGIC *provides extensive options for implementing custom logic
-- MAGIC
-- MAGIC Let us merge the records from Accessories_2 into Accessories_1
-- MAGIC
-- MAGIC We will check if the Part exist in the target table and will update the Quantity column using *WHEN MATCHED* , else we will insert the missing record using *WHEN NOT MATCHED*

-- COMMAND ----------

SELECT * FROM Computer.Accessories_1

-- COMMAND ----------

SELECT * frOm Computer.Accessories_2
--Note that record with Part 'External Drive' and 'Headphones' are matching records so we will add up the quantity for these two records and insert the rest of the records

-- COMMAND ----------

MERGE INTO Computer.Accessories_1 a
USING Computer.Accessories_2 b
ON a.Part = b.Part
WHEN MATCHED AND b.Quantity IS NOT NULL 
THEN UPDATE SET a.Quantity = a.Quantity + b.Quantity
WHEN NOT MATCHED THEN INSERT *

-- COMMAND ----------

--Review the outcome
SELECT * frOm Computer.Accessories_1

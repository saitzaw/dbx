-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### - Join Tables
-- MAGIC
-- MAGIC Spark SQL supports standard join operations (inner, outer, left, right, anti, cross, semi)
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### - Set Operators
-- MAGIC Spark SQL supports **UNION**, **MINUS**, and **INTERSECT** set operators.
-- MAGIC
-- MAGIC **UNION** returns the collection of two queries.
-- MAGIC
-- MAGIC **MINUS** returns the result of substraction of datasets
-- MAGIC
-- MAGIC **INTERSECT** returns the common data between two datasets

-- COMMAND ----------

--CREATING Datasets to show the examples for Join and Set Operations
DROP DATABASE IF EXISTS Computer CASCADE;
CREATE DATABASE Computer;

CREATE OR REPLACE TABLE Computer.Accessories_Part_1
(  ID INT,  Part STRING,  Color STRING);
INSERT INTO Computer.Accessories_Part_1
VALUES (1, 'Mouse', 'Black'), (2, 'Keyboard', 'White'), (3, 'Monitor', 'Red');

CREATE OR REPLACE TABLE Computer.Accessories_Part_2
(  ID INT,  Part STRING,  Color STRING);
INSERT INTO Computer.Accessories_Part_2
VALUES (1, 'Mouse', 'Black'),(4, 'Cables', 'Black'), (5, 'Headphones', 'White'), (6, 'Printer', 'White');

CREATE OR REPLACE TABLE Computer.Accessories_Price
(  ID INT,  Price FLOAT,  Manufacturer STRING);
INSERT INTO Computer.Accessories_Price
VALUES (1, 5.2, 'Sydney'), (2, 4.99, 'Melbourne'), (3, 4.99, 'Perth'), (4, 2.99, 'Sydney'), (5, 10.99, 'Perth'), (6, 6.99, 'Brisbane');

-- COMMAND ----------

SELECT * FROM Computer.Accessories_Part_1;

-- COMMAND ----------

SELECT * FROM Computer.Accessories_Part_2;

-- COMMAND ----------

SELECT * FROM Computer.Accessories_Price;

-- COMMAND ----------

--Inner join example
SELECT 
  Part.*
  , Price.Price
  , Price.Manufacturer 
FROM Computer.Accessories_Part_1 Part
INNER JOIN Computer.Accessories_Price Price
ON Part.ID = Price.ID

-- COMMAND ----------

-- right join 

SELECT 
  Part.*
  , Price.Price
  , Price.Manufacturer 
FROM Computer.Accessories_Part_1 Part
RIGHT OUTER JOIN Computer.Accessories_Price Price
ON Part.ID = Price.ID

-- COMMAND ----------

-- left join 
select 
* 
from Computer.Accessories_Part_1 Part
left join Computer.Accessories_Price Price
on Part.ID = Price.ID; 

-- COMMAND ----------

-- left semi join 
SELECT 
*
FROM Computer.Accessories_Part_1 Part
LEFT SEMI JOIN Computer.Accessories_Price Price
ON Part.ID = Price.ID

-- COMMAND ----------

-- left anti-join example 
SELECT 
*
FROM Computer.Accessories_Part_1 Part
LEFT ANTI JOIN Computer.Accessories_Price Price
ON Part.ID = Price.ID

-- COMMAND ----------

-- left anti join with more data 
select 
* 
from Computer.Accessories_Part_2 p2
left anti join Computer.Accessories_Part_1 p1
on p2.ID = p1.ID

-- COMMAND ----------

SELECT * FROM Computer.Accessories_Part_1
UNION
SELECT * FROM Computer.Accessories_Part_2;

-- COMMAND ----------

-- set A / set B
SELECT * FROM Computer.Accessories_Part_1
EXCEPT
SELECT * FROM Computer.Accessories_Part_2;

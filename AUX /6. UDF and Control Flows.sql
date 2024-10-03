-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### UDF - User Defined Functions
-- MAGIC A user-defined function (UDF) is a function defined by a user, allowing custom logic to be reused in the user environment.
-- MAGIC
-- MAGIC ####Control Flow - CASE/WHEN
-- MAGIC
-- MAGIC The standard SQL syntactic construct **`CASE`** / **`WHEN`** allows the evaluation of multiple conditional statements with alternative outcomes based on table contents.

-- COMMAND ----------

--CREATING a Dataset to demonstrate UDF
DROP DATABASE IF EXISTS Computer CASCADE;
CREATE DATABASE Computer;

CREATE OR REPLACE TABLE Computer.Accessories
(
  ID INT,
  Part STRING,
  Color STRING
);
INSERT INTO Computer.Accessories
VALUES (1, 'Mouse', 'Black'), (2, 'Keyboard', 'White'), (3, 'Monitor', 'Red');
SELECT * FROM Computer.Accessories;

-- COMMAND ----------

--Creating a (UDF)USER DEFINED FUNCTION GenerateItemString that takes two strings as an input and returns a concatenated string
-- add the custom function here
CREATE OR REPLACE FUNCTION Computer.GenerateItemString(text1 STRING, text2 STRING)
RETURNS STRING
  RETURN concat(text1, " is of ", text2, "color")

-- COMMAND ----------

--Calling a UDF GenerateItemString
-- check in computer it will be in custom UC 
SELECT Id, Part, Color, Computer.GenerateItemString(Part, Color) as Description  FROM Computer.Accessories;

-- COMMAND ----------

DESCRIBE FUNCTION Computer.GenerateItemString

-- COMMAND ----------

--See the UDF properties and definition
DESCRIBE FUNCTION EXTENDED Computer.GenerateItemString

-- COMMAND ----------

SELECT *, 
CASE 
  WHEN Color == 'Black' THEN 'I Love it'
  WHEN Color == 'White' THEN 'I like it'
  ELSE 'I am Ok'
END AS MyLikes
FROM Computer.Accessories;

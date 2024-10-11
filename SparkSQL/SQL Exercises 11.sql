-- Databricks notebook source
-- MAGIC %md 
-- MAGIC ### Check the Table and database properties

-- COMMAND ----------

use catalog dbx_sthz; 
use schema db_demo; 

-- COMMAND ----------

describe department;

-- COMMAND ----------

describe external location department;

-- COMMAND ----------

describe database db_demo;


-- COMMAND ----------

CREATE TABLE my_table (
  id INT COMMENT 'Unique Identification Number', 
  name STRING COMMENT 'PII', 
  age INT COMMENT 'PII') 
TBLPROPERTIES ('contains_pii'=True) 
COMMENT 'Contains PII'; 

-- COMMAND ----------

DESCRIBE TABLE my_table

-- COMMAND ----------

insert into my_table values
 (1, 'Alice', 21),
 (2, 'Bob', 23);

-- COMMAND ----------

select * from my_table;

-- COMMAND ----------



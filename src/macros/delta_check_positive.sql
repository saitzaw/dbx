-- Databricks notebook source
-- This notebook is for testing 
use catalog dbx_arch; 
use schema macros;
create function check_positive(column_name float) 
returns boolean 
comment 'to check the positive value or not'
language sql
return (column_name > 0) 

-- COMMAND ----------

-- test 
select check_positive(100) as positive

-- COMMAND ----------

-- negative value
select check_positive(-100) as positive

-- COMMAND ----------



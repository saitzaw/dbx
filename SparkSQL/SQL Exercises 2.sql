-- Databricks notebook source
-- MAGIC %md
-- MAGIC ###  recursive in SQl 
-- MAGIC
-- MAGIC - find the level 
-- MAGIC - find the decentant 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### use UC and schema

-- COMMAND ----------

use catalog dbx_sthz; 
use schema db_demo; 

-- COMMAND ----------

drop table if exists partent_of; 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### create a table

-- COMMAND ----------

create table if not exists parent_of 
(
  parent string
  , child string
  , date_of_birth date
)

-- COMMAND ----------

insert into parent_of (parent, child, date_of_birth) values 
('Alice', 'Carol', '1945')
, ('Bob', 'Carol', '1945')
, ('Carol', 'George', '1970')
, ('Carol', 'Dave', '1972')
, ('Dave', 'Marry', '2000')
, ('Eve', 'Marry', '2000')
, ('Marry', 'Frank', '2022')

-- COMMAND ----------

select * from parent_of; 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Spark SQL format 
-- MAGIC - It is very difference from Postgresql or other SQL format 
-- MAGIC - It does not support recursive method in cte 

-- COMMAND ----------

with current_ancestors as (
  select
    child || '->' || parent as lineage,
    parent,
    1 as level
  from
    parent_of
  where
    child = 'Frank'
),
consecutive_ancestors as (
  select
    p1.child || '->' || p1.parent as lineage,
    p1.parent,
    1 + c.level as level
  from
    parent_of p1
    cross join current_ancestors c on p1.child = c.parent
)
select
  *
from
  current_ancestors
union all
select
  *
from
  consecutive_ancestors;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### descendent 

-- COMMAND ----------

with current_descendent as (
  select
    child,
    parent,
    1 as level
  from
    parent_of
  where
    parent = 'Alice'
),
consecutive_descendent as (
  select
    p1.child,
    p1.parent,
    1 + c.level as level
  from
    parent_of p1
    cross join current_descendent c on c.child = p1.parent
)
select
  *
from
  current_descendent
union all
select
  *
from
  consecutive_descendent;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### view table to use as recursive table

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW acentors_temp AS
SELECT parent FROM parent_of WHERE child = 'Carol';

-- COMMAND ----------

-- loop into the acentors_temp table
insert into acentors_temp 
SELECT p.parent FROM parent_of p
INNER JOIN acentors_temp a ON p.child = a.parent;

-- COMMAND ----------

-- 

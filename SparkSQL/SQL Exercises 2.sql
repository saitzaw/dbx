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

truncate table parent_of;

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

-- not run this cell 
-- this only support in sql table 
-- not support in spark
-- with cte_ascentor as (
--   select 
--   	parent, 
--   	child, 
--   	parent || '->' || child as lineage,  
--   	1 as level,
--   	date_of_birth
--   from 
--   	parent_of 
--   where 
--   	child = 'Frank' 
--   UNION ALL 
--   select 
--   	p.parent, 
--   	p.child,
--  	p.parent || '->' || c.child as lineage,
--   	1 + c.level as level,
-- 	p.date_of_birth
--   from 
--   	parent_of p 
--   cross join 
--   	cte_ascentor c 
--   on p.child = c.parent   
-- )
-- select * from cte_ascentor; 


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

-- to find the descendent 
-- with descendent as (
--   select child, parent , 1 as level from parent_of where parent = 'Alice'
--   union all 
--   select p1.child, p1.parent, 1 + c.level as level from parent_of p1 cross join descendent d on p1.child = d.parent
-- )
-- select * from descendent; 

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
-- ON Azure it will be a partial scan
-- insert into acentors_temp 
-- SELECT p.parent FROM parent_of p
-- INNER JOIN acentors_temp a ON p.child = a.parent;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### PySpark 
-- MAGIC - recursive is not support in Databricks 
-- MAGIC - using the pyspark to get the union with recurive nature [looping]

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC dataDF = spark.sql("select * from dbx_sthz.db_demo.parent_of")

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC dataDF.show()

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC from pyspark.sql.functions import col, lit, concat, datediff
-- MAGIC # choose the root dataframe 
-- MAGIC root = 'Frank'
-- MAGIC currentDF = dataDF.where(col("child") == root).select(
-- MAGIC   "parent", 
-- MAGIC   "child", 
-- MAGIC   lit(1).alias("level"), 
-- MAGIC   concat(col("parent"), lit(" -> "), col("child")).alias("lineage"),
-- MAGIC   col("date_of_birth").alias("child_date_of_birth")
-- MAGIC   )

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC from pyspark.sql.functions import date_sub # This line is added to fix the error
-- MAGIC allAncestorsDF = currentDF
-- MAGIC while True: 
-- MAGIC     nextDF = dataDF.alias("df1").join(currentDF.alias("df2"), col("df2.parent") == col("df1.child"), "inner") \
-- MAGIC     .select(
-- MAGIC         col("df1.parent"),
-- MAGIC         col("df1.child"),
-- MAGIC         (col("df2.level") + 1).alias("level"), 
-- MAGIC         concat(col("df1.parent"), lit(" -> "), col("df1.child")).alias("lineage"),
-- MAGIC         col("df1.date_of_birth").alias("child_date_of_birth")
-- MAGIC     )
-- MAGIC     if nextDF.count() == 0: # Fixed the condition to check nextDF instead of currentDF
-- MAGIC         break
-- MAGIC     allAncestorsDF = allAncestorsDF.union(nextDF).distinct()
-- MAGIC     currentDF = nextDF

-- COMMAND ----------

-- MAGIC %python
-- MAGIC allAncestorsDF.where(col("parent").isin(*[row.child for row in allAncestorsDF.select("child").distinct().collect()])).show()

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC allAncestorsDF.display()

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC import org.apache.spark.sql.functions.{col,lit,concat}

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC val dataDFs = spark.sql("select * from dbx_sthz.db_demo.parent_of")

-- COMMAND ----------

-- MAGIC %scala 
-- MAGIC var root = "Frank"
-- MAGIC var rootDFs = dataDFs.filter(dataDFs("child") === root).select(
-- MAGIC   dataDFs("parent"),
-- MAGIC   dataDFs("child"),
-- MAGIC   lit(1).alias("level"),
-- MAGIC   concat(dataDFs("parent"), lit("->"), dataDFs("child")).alias("lineage"),
-- MAGIC   dataDFs("date_of_birth")
-- MAGIC )

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC rootDFs.show()

-- COMMAND ----------

-- MAGIC %scala 
-- MAGIC var allAncestorsDF = rootDFs
-- MAGIC var loop = true
-- MAGIC var nextDF = rootDFs
-- MAGIC while (loop) {
-- MAGIC     nextDF = dataDFs.alias("df1").join(rootDFs.alias("df2"), col("df2.parent") === col("df1.child"), "inner")
-- MAGIC     .select(
-- MAGIC         col("df1.parent"),
-- MAGIC         col("df1.child"),
-- MAGIC         (col("df2.level") + 1).alias("level"), 
-- MAGIC         concat(col("df1.parent"), lit(" -> "), col("df1.child")).alias("lineage"),
-- MAGIC         col("df1.date_of_birth").alias("child_date_of_birth")
-- MAGIC     )
-- MAGIC     if (nextDF.count() == 0) {
-- MAGIC         loop = false
-- MAGIC     } else {
-- MAGIC         allAncestorsDF = allAncestorsDF.union(nextDF).distinct()
-- MAGIC         rootDFs = nextDF
-- MAGIC     }
-- MAGIC }
-- MAGIC         

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC allAncestorsDF.show()

-- COMMAND ----------



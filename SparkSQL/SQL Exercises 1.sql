-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Tree 
-- MAGIC - find leaf node 
-- MAGIC - inner node, and 
-- MAGIC - root node from the table 

-- COMMAND ----------

use catalog dbx_sthz; 
create schema if not exists db_demo; 

-- COMMAND ----------

use schema db_demo;
create table if not exists trees 
(
  child integer
  , parent integer
  );

-- COMMAND ----------

truncate table trees;

-- COMMAND ----------

insert into trees values 
(1, 2)
, (3, 2)
, (6, 8)
, (9, 8)
, (2, 5)
, (8, 5)
, (5, null);


-- COMMAND ----------

select * from trees;

-- COMMAND ----------

with cte_inner as (
  select
    distinct t.child as node_name,
    'inner' as node_type
  from
    trees t
    inner join trees t1 on t.child = t1.parent
  where
    t.parent is not null
),
cte_leaf as (
  select
    t.child as node_name,
    'leaf' as node_type
  from
    trees t
  where
    t.parent is not null
    and t.child not in (
      select
        node_name
      from
        cte_inner
    )
),
cte_root as (
  select
    trees.child as node_name,
    'root' as node_type
  from
    trees
  where
    parent is null
)
select
  *
from
  cte_leaf
union
select
  *
from
  cte_inner
union
select
  *
from
  cte_root
order by
  node_name;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### PySpark
-- MAGIC - create a dataframe for inner node 
-- MAGIC - create a dataframe for leaf node 
-- MAGIC - create a dataframe for root node 
-- MAGIC - union all dataframe

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC from pyspark.sql import functions as F
-- MAGIC treeDF = spark.sql("select * from trees")

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC treeDF.show()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Inner 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC cloneTreeDF = treeDF.alias("cloneTree")
-- MAGIC cloneTreeDF = cloneTreeDF.withColumnRenamed("child","c_child").withColumnRenamed("parent","c_parent")
-- MAGIC # Perform the join using the aliased DataFrames
-- MAGIC innerNodeDF = treeDF.join(
-- MAGIC     cloneTreeDF, 
-- MAGIC     treeDF["child"] == cloneTreeDF["c_parent"],
-- MAGIC     'inner'
-- MAGIC ).where(treeDF.parent.isNotNull()).select(
-- MAGIC    cloneTreeDF["c_parent"]
-- MAGIC ).distinct()
-- MAGIC
-- MAGIC # Display the result

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### leaf node

-- COMMAND ----------

-- MAGIC  %python
-- MAGIC leafNodeDF =  treeDF.join(
-- MAGIC     innerNodeDF, 
-- MAGIC     treeDF["child"] == innerNodeDF["c_parent"],
-- MAGIC     'leftanti'
-- MAGIC ).where(treeDF.parent.isNotNull()).select(
-- MAGIC    treeDF["child"]
-- MAGIC ).distinct() 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC leafNodeDF = leafNodeDF.withColumn("node_name", F.lit("leaf"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### root node

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC rootNodeDF = treeDF.where(treeDF.parent.isNull()).select("child").withColumn("node_name", F.lit("root"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### prepare for inner node 

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC innerNodeDFAlias = innerNodeDF.withColumnRenamed("c_parent", "child").withColumn("node_name", F.lit("inner"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### UNION

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC rootNodeDF.union(innerNodeDFAlias).union(leafNodeDF).show()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Scala

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC import org.apache.spark.sql.functions.{lit, col}
-- MAGIC val treeDFs = spark.sql("select * from dbx_sthz.db_demo.trees")

-- COMMAND ----------

-- MAGIC %scala 
-- MAGIC treeDFs.show()

-- COMMAND ----------

-- MAGIC %scala 
-- MAGIC val treeDFsAlias = treeDFs.withColumnRenamed("child", "c_child").withColumnRenamed("parent", "c_parent"); 
-- MAGIC val  innerDFs = treeDFs.join(treeDFsAlias, treeDFs("child") === treeDFsAlias("c_parent"), "inner").where(treeDFs("parent").isNotNull).select(treeDFsAlias("c_parent")).distinct()

-- COMMAND ----------

-- MAGIC %scala 
-- MAGIC val leafDFs = treeDFs.join(innerDFs, treeDFs("child") === innerDFs("c_parent"),"leftanti").where(treeDFs("parent").isNotNull).select(treeDFs("child"))

-- COMMAND ----------

-- MAGIC %scala 
-- MAGIC val newInnerDFs = innerDFs.withColumnRenamed("c_parent", "child").withColumn("node_name", lit("inner"))
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC val rootDFs = treeDFs.where(treeDFs("parent").isNull).select(treeDFs("child")).withColumn("node_name", lit("root")) 

-- COMMAND ----------

-- MAGIC %scala 
-- MAGIC rootDFs.union(newInnerDFs).union(leafDFs.withColumn("node_name", lit("leaf"))).show()

-- COMMAND ----------



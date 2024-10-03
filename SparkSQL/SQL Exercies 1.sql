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

truncate table trees;

-- COMMAND ----------

use schema db_demo;
create table if not exists trees 
(
  child integer
  , parent integer
  );

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



-- Databricks notebook source
use catalog dbx_sthz;
use schema db_demo;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Leetcode 50 
-- MAGIC - having more friends
-- MAGIC - logics 
-- MAGIC   - union all ids 
-- MAGIC   - group by ids 
-- MAGIC   - count id 
-- MAGIC   - select 

-- COMMAND ----------

-- create table 
create table if not exists request_accepted (
  requester_id int not null,
  accepter_id int, 
  accept_date date); 



-- COMMAND ----------

insert into request_accepted (requester_id, accepter_id, accept_date) values 
('1', '2', '2016-06-03'),
('1', '3', '2016-06-08'),
('2', '3', '2016-06-08'),
('3', '4', '2016-06-09');

-- COMMAND ----------

select * from request_accepted;

-- COMMAND ----------



-- COMMAND ----------

with cte_requesters as (
select requester_id as id from request_accepted
union all
select accepter_id as id from request_accepted
) 
select id, count(id) from cte_requesters group by id; 

-- COMMAND ----------

-- test case 
insert into request_accepted (requester_id, accepter_id, accept_date) values 
('4', '5', '2016-06-09'),
('4', '6', '2016-06-09');

-- COMMAND ----------

with cte_requesters as (
select requester_id as id from request_accepted
union all
select accepter_id as id from request_accepted
) 
select id, count(id) from cte_requesters group by id order by id; 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### PySpark
-- MAGIC - read from the UC 
-- MAGIC - flatten the table 
-- MAGIC - using the group by 

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC moreFriendDF = spark.sql("select * from dbx_sthz.db_demo.request_accepted")
-- MAGIC moreFriendDF.show()

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC from pyspark.sql import functions as F
-- MAGIC moreFriendDF\
-- MAGIC   .select(F.col("requester_id").alias("id"))\
-- MAGIC     .union(moreFriendDF.select(F.col("accepter_id").alias("id")))\
-- MAGIC       .groupBy(F.col("id"))\
-- MAGIC         .agg(F.count("id").alias("total_count"))\
-- MAGIC           .orderBy(F.col("id")\
-- MAGIC             .asc())\
-- MAGIC               .show()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### scala

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC // trying in scala 
-- MAGIC val newFriendDFs = spark.sql("select * from dbx_sthz.db_demo.request_accepted")

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC newFriendDFs.show()

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC import org.apache.spark.sql.functions.{col, count} 
-- MAGIC newFriendDFs
-- MAGIC .select(col("requester_id")alias("id"))
-- MAGIC .union(newFriendDFs.select(col("accepter_id").alias("id")))
-- MAGIC .groupBy(col("id"))
-- MAGIC .agg(count("id").alias("total_count"))
-- MAGIC .orderBy("id")
-- MAGIC .show()

-- COMMAND ----------



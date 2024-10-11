-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### LeetCode exercies
-- MAGIC - exchaged seat 
-- MAGIC - subquery 

-- COMMAND ----------

use catalog dbx_sthz;
use schema db_demo;

-- COMMAND ----------

-- create a table in the db_demo schema
create table If not exists seat (
  id int,
  student varchar(255)
  ); 


-- COMMAND ----------

insert into seat (id, student) values 
('1', 'Abbot'),
('2', 'Doris'),
('3', 'Emerson'),
('4', 'Green'),
('5', 'Jeames'); 

-- COMMAND ----------

select
  id, 
  student, 
  case 
    when id % 2 = 0 
      then lag(student) over(order by id)
      else coalesce(lead(student) over(order by id), student)
  end as reorder
from seat

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # using PySpark to calculate that result 
-- MAGIC # using Lead and Lag for this problem 
-- MAGIC data = [(1, 'Abbot'), (2, 'Doris'), (3, 'Emerson'), (4, 'Green'), (5, 'Jeames')]
-- MAGIC seatDF = spark.createDataFrame(
-- MAGIC     data, 
-- MAGIC     schema='id int,student string')
-- MAGIC     

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC seatDF.show()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql import functions as F
-- MAGIC from pyspark.sql.window import Window
-- MAGIC
-- MAGIC windowSpec = Window.orderBy("id")
-- MAGIC seatDF.withColumn(
-- MAGIC     "reorder",
-- MAGIC     F.when(F.col("id") % 2 == 0, F.lag("student").over(windowSpec)).otherwise(
-- MAGIC         F.coalesce(F.lead("student").over(windowSpec), F.col("student"))
-- MAGIC     ),
-- MAGIC ).show()

-- COMMAND ----------

-- MAGIC %scala 
-- MAGIC import org.apache.spark.sql.functions.{lit, col, when, lead, lag, coalesce}
-- MAGIC import org.apache.spark.sql.expressions.Window
-- MAGIC

-- COMMAND ----------

-- MAGIC %scala 
-- MAGIC val windowSpec = Window.orderBy("id")
-- MAGIC var studentDF = spark.sql("select * from dbx_sthz.db_demo.seat")
-- MAGIC studentDF.withColumn(
-- MAGIC   "reorder", 
-- MAGIC   when(col("id") %2 === 0, lag(col("student"), 1).over(windowSpec))
-- MAGIC   .otherwise(coalesce(lead(col("student"), 1).over(windowSpec), lit(null))))
-- MAGIC   .show()

-- COMMAND ----------



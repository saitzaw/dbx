-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Even and Odd 
-- MAGIC https://datalemur.com/blog/google-sql-interview-questions 

-- COMMAND ----------

use catalog dbx_sthz; 
use schema db_demo;

-- COMMAND ----------

create table if not exists even_odd_measurements 
(measurement_id int,	measurement_value float, 	measurement_time timestamp);

-- COMMAND ----------

insert into even_odd_measurements values 
(131233,	1109.51,	'2024-10-07 09:00:00'), 
(135211,	1662.74,	'2024-10-07 11:00:00'),
(523542,	1246.24,	'2024-10-07 13:15:00'),
(143562,	1124.50,	'2024-11-07 15:00:00'),
(346462,	1234.14,	'2024-11-07 16:45:00');

-- COMMAND ----------

select * from even_odd_measurements;

-- COMMAND ----------

select row_number() over(partition by substr(measurement_time, 0,10) order by measurement_time) as rn
 from even_odd_measurements; 

-- COMMAND ----------

with  cte_even_odd as (
  select 
  substr(measurement_time, 0,10) as measurement_date
  ,  measurement_value as measurement_value
  , row_number() over(partition by substr(measurement_time, 0,10) order by measurement_time) as rn
 from 
 even_odd_measurements
) select 
  measurement_date
  , sum(case when rn %2 = 0 then measurement_value else 0 end) as even_sum
  , sum(case when rn %2 != 0 then measurement_value else 0 end) as odd_sum
  from cte_even_odd
  group by measurement_date; 

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC from pyspark.sql.functions import when, col, sum, row_number, substring, round
-- MAGIC from pyspark.sql.window import Window

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC windowSpec = Window.partitionBy("measurement_date").orderBy("measurement_time")

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC dataDF = spark.sql("select * from dbx_sthz.db_demo.even_odd_measurements")

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC rootDF = dataDF.withColumn("measurement_date", substring(col("measurement_time"), 0, 10)).withColumn("rn", row_number().over(windowSpec))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC rootDF.groupBy("measurement_date").agg(
-- MAGIC   round(
-- MAGIC     sum(
-- MAGIC       when(
-- MAGIC         col("rn") % 2 == 0, col("measurement_value")
-- MAGIC         ).otherwise(0)
-- MAGIC     ), 2).alias("even_sum"),
-- MAGIC     round(
-- MAGIC     sum(
-- MAGIC       when(
-- MAGIC         col("rn") % 2 != 0, col("measurement_value")
-- MAGIC         ).otherwise(0)
-- MAGIC     ), 2).alias("odd_sum")
-- MAGIC ).display()
-- MAGIC

-- COMMAND ----------

-- MAGIC %scala 
-- MAGIC import org.apache.spark.sql.functions.{when, sum, substring, col, row_number, round}
-- MAGIC import org.apache.spark.sql.expressions.Window

-- COMMAND ----------

-- MAGIC %scala 
-- MAGIC var dataDFs = spark.sql("select * from dbx_sthz.db_demo.even_odd_measurements")

-- COMMAND ----------

-- MAGIC %scala 
-- MAGIC val windowSpec = Window.partitionBy("measurement_date").orderBy("measurement_time")

-- COMMAND ----------

-- MAGIC %scala 
-- MAGIC var rootDFs = dataDFs.withColumn("measurement_date", substring(col("measurement_time"), 0, 10))
-- MAGIC .withColumn("rn", row_number().over(windowSpec))
-- MAGIC

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC rootDFs.groupBy("measurement_date").agg(
-- MAGIC   round(
-- MAGIC     sum(
-- MAGIC       when(
-- MAGIC         col("rn") %2 === 0, col("measurement_value")
-- MAGIC         ).otherwise(0)
-- MAGIC       ), 2
-- MAGIC     ).alias("even_sum"),
-- MAGIC     round(
-- MAGIC     sum(
-- MAGIC       when(
-- MAGIC         col("rn") %2 =!= 0, col("measurement_value")
-- MAGIC         ).otherwise(0)
-- MAGIC       ), 2
-- MAGIC     ).alias("odd_sum")
-- MAGIC   ).show()

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC var evenDFs = rootDFs.groupBy("measurement_date").agg(
-- MAGIC   round(
-- MAGIC     sum(
-- MAGIC       when(
-- MAGIC         col("rn") %2 === 0, col("measurement_value")
-- MAGIC         ).otherwise(0)
-- MAGIC       ), 2
-- MAGIC     ).alias("even_sum")
-- MAGIC   )

-- COMMAND ----------

-- MAGIC %scala 
-- MAGIC var oddDFs = rootDFs.groupBy("measurement_date").agg(
-- MAGIC   round(
-- MAGIC     sum(
-- MAGIC       when(
-- MAGIC         col("rn") %2 =!= 0, col("measurement_value")
-- MAGIC         ).otherwise(0)
-- MAGIC       ), 2
-- MAGIC     ).alias("odd_sum")
-- MAGIC   )

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC evenDFs.join(oddDFs, "measurement_date").show()

-- COMMAND ----------



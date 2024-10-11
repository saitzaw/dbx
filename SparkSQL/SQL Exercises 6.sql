-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Rolling average 
-- MAGIC - calculation of rolling average
-- MAGIC
-- MAGIC | # | language | method | 
-- MAGIC |:-:|:-------:|:-------:|
-- MAGIC |1 | SparkSQL | rows between 2 preceding and current row | 
-- MAGIC |2 | PySpark | rowsBetween(-2, 0) |
-- MAGIC |3 | Scala  | rowsBetween(-2,0) |
-- MAGIC
-- MAGIC
-- MAGIC https://datalemur.com/questions/rolling-average-tweets

-- COMMAND ----------

-- Rolling average 
use catalog dbx_sthz; 
use schema db_demo; 

-- COMMAND ----------

create table if not exists tweets (user_id int, tweet_date timestamp, 	tweet_count int)

-- COMMAND ----------

insert into tweets (user_id, tweet_date, tweet_count) values 
(111, '2022-01-06 00:00:00',	2),
(111,	'2022-02-06 00:00:00',	1),
(111,	'2022-03-06 00:00:00',	3),
(111,	'2022-04-06 00:00:00',	4),
(111,	'2022-05-06 00:00:00',	5);

-- COMMAND ----------

select 
  user_id, 
  tweet_date, 
  tweet_count, 
  round(avg(tweet_count) over(partition by user_id order by tweet_date rows between 2 preceding and current row), 2) as moving_average 
from tweets; 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## PySpark

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC from pyspark.sql.functions import col, lead, lag, avg, round
-- MAGIC from pyspark.sql.window import Window 

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC windowSpec = Window.partitionBy("user_id").orderBy("tweet_date").rowsBetween(-2,0)

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC tweetDF =spark.sql("select * from tweets")
-- MAGIC tweetDF.select(
-- MAGIC     "user_id",
-- MAGIC     "tweet_date",
-- MAGIC     "tweet_count",
-- MAGIC     round(avg("tweet_count").over(windowSpec), 2).alias("moving_average"))\
-- MAGIC     .display()

-- COMMAND ----------

-- MAGIC %scala 
-- MAGIC import org.apache.spark.sql.functions.{round, avg, col}
-- MAGIC import org.apache.spark.sql.expressions.Window
-- MAGIC val windowSpec = Window.partitionBy("user_id").orderBy("tweet_date").rowsBetween(-2,0) 
-- MAGIC

-- COMMAND ----------

-- MAGIC %scala 
-- MAGIC var tweetDFs = spark.sql("select * from tweets") 

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC tweetDFs.select(
-- MAGIC   $"user_id",
-- MAGIC   $"tweet_date", 
-- MAGIC   $"tweet_count", 
-- MAGIC   round(avg($"tweet_count").over(windowSpec), 2).alias("moving_average")
-- MAGIC   ).show()

-- COMMAND ----------



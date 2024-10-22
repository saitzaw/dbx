-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## windows in SQL
-- MAGIC https://mode.com/sql-tutorial/sql-window-functions 

-- COMMAND ----------

use catalog dbx_arch; 
use schema default;

-- COMMAND ----------

select * from `2014_q_1_capitalbikeshare_tripdata`;

-- COMMAND ----------

select count(*) from 2014_q_1_capitalbikeshare_tripdata;

-- COMMAND ----------

select min(start_date), max(start_date) from 2014_q_1_capitalbikeshare_tripdata; 

-- COMMAND ----------

-- alter table 2014_q_1_capitalbikeshare_tripdata rename column `End station` to end_station;

-- COMMAND ----------

-- select * from `2014_q_1_capitalbikeshare_tripdata` limit 10; 

-- COMMAND ----------

-- alter table `2014_q_1_capitalbikeshare_tripdata`  rename column `Bike number` to bike_number; 

-- COMMAND ----------

-- alter table `2014_q_1_capitalbikeshare_tripdata` rename column `Member type` to member_type; 

-- COMMAND ----------

-- getting total in SQL 
select 
  duration,
  sum(duration) over (order by start_date) as running_total 
from `2014_q_1_capitalbikeshare_tripdata`; 

-- COMMAND ----------

-- window based on start terminal 
select 
  duration,
  sum(duration) over (partition by start_station_number) as total_start_station
from `2014_q_1_capitalbikeshare_tripdata`
where start_date < '2014-01-02'; 

-- COMMAND ----------

-- round test 
select round(4.5, 2)

-- COMMAND ----------

-- checking with more windows functions 
select 
  duration
  , sum(duration) over (partition by start_station_number) as total_start_station
  , count(duration) over (partition by start_station_number) as count_start_station
  , round(avg(duration) over (partition by start_station_number), 4) as average_start_station
from `2014_q_1_capitalbikeshare_tripdata`
where start_date < '2014-01-02';

-- COMMAND ----------

select 
  start_station_number
  , start_date
  , row_number() over (partition by start_station_number order by start_date) as rn
from `2014_q_1_capitalbikeshare_tripdata`
where start_date < '2014-01-02';

-- COMMAND ----------

-- window with lead and lag

-- COMMAND ----------

--  windows with same partition
select 
  start_station
  , duration
  , ntile(4) over (partition by start_station_number order by duration) as quantile_duration
  , ntile(5) over (partition by start_station_number order by duration) as quintle_duration_5bins
  , ntile(100) over (partition by start_station_number order by duration) as percentile_duration
from `2014_q_1_capitalbikeshare_tripdata`
order by start_station_number, duration desc;

-- COMMAND ----------

--  windows alias 
select 
  start_station
  , duration
  , ntile(4) over w as quantile_duration
  , ntile(5) over w as quintle_duration_5bins
  , ntile(100) over w as percentile_duration
from `2014_q_1_capitalbikeshare_tripdata`
window w as (partition by start_station_number order by duration)
order by start_station_number, duration desc;

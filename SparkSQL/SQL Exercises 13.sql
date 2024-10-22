-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### UDF function 
-- MAGIC - Testing UDF Function as DRY and reusable 
-- MAGIC - create UDF function in UC 
-- MAGIC - use the UDF function for single column 
-- MAGIC - use the UDF function for whole table 

-- COMMAND ----------

-- use UC 
use catalog dbx_arch; 

-- COMMAND ----------

-- create a new schema for macrs
create schema if not exists dbx_arch.macros;

-- COMMAND ----------

-- create a new table 

-- COMMAND ----------

-- test sql 
CREATE OR REPLACE FUNCTION dbx_arch.macros.blue()
  RETURNS STRING
  COMMENT 'Blue color code'
  LANGUAGE SQL
  RETURN '0000FF'

-- COMMAND ----------

-- calling the macro function namely blue
select dbx_arch.macros.blue();

-- COMMAND ----------

-- create a user defined function with parameter and return type
create or replace function dbx_arch.macros.to_hex(x int comment 'Any number from 0 - 255 ')
  returns string
  comment 'converts a decimal to a hexadecimal'
  contains sql deterministic
  return lpad(hex(least(greatest(0,x), 255)), 2, 0)

-- COMMAND ----------

select dbx_arch.macros.to_hex(12); 

-- COMMAND ----------

SELECT dbx_arch.macros.to_hex(id) FROM range(2);

-- COMMAND ----------

EXPLAIN SELECT dbx_arch.macros.to_hex(id) FROM range(2);

-- COMMAND ----------

create or replace function dbx_arch.macros.from_rgb(rgb STRING comment 'an RGB hex color code')
  returns string 
  comment 'translates an RGB color code into a color name'
  return decode(rgb, 'FF00FF', 'magenta', 
                      'FF080', 'rose');

-- COMMAND ----------

select dbx_arch.macros.from_rgb('FF00FF');

-- COMMAND ----------

-- create a new schema file path here 
create schema raws;

-- COMMAND ----------

CREATE OR REPLACE TABLE dbx_arch.raws.colors(rgb STRING NOT NULL, name STRING NOT NULL);
INSERT INTO dbx_arch.raws.colors VALUES
  ('FF00FF', 'magenta'),
  ('FF0080', 'rose'),
  ('BFFF00', 'lime'),
  ('7DF9FF', 'electric blue');

-- COMMAND ----------

SELECT dbx_arch.macros.from_rgb(rgb) 
  FROM VALUES('7DF9FF'),
  ('BFFF00') AS codes(rgb);

-- COMMAND ----------

CREATE OR REPLACE FUNCTION
dbx_arch.macros.from_rgb(rgb STRING COMMENT 'an RGB hex color code') 
   RETURNS STRING
   READS SQL DATA SQL SECURITY DEFINER
   COMMENT 'Translates an RGB color code into a color name'
   RETURN SELECT FIRST(name) FROM dbx_arch.raws.colors WHERE rgb = from_rgb.rgb;

-- COMMAND ----------

select dbx_arch.macros.from_rgb('7DF9FF');

-- COMMAND ----------

SELECT dbx_arch.macros.from_rgb(rgb) 
  FROM VALUES('7DF9FF'),
  ('BFFF00') AS codes(rgb);

-- COMMAND ----------

DESCRIBE FUNCTION dbx_arch.macros.from_rgb;

-- COMMAND ----------

-- testing unit converting example 
create function con_f_to_c(unit string, temp double) 
returns double
comment 'Convert ferenheit to celsius'
language sql
return case unit when 'f' then (temp - 32) * 5/9 else temp end;

-- COMMAND ----------

select con_f_to_c('f', 100) as celsius;

-- COMMAND ----------

-- create a function for rolling dice 
create function dbx_arch.macros.roll_dice() 
returns int
comment 'Roll a dice'
language sql
return (rand() * 6)::int;

-- COMMAND ----------

-- 
select dbx_arch.macros.roll_dice() as outcome;

-- COMMAND ----------

create function dbx_arch.raws.selected_bikeshared_data()
returns table 
comment 'Bikeshare data'
return 
select 
  duration
  , start_date
  , end_date
  , start_station_number
  , end_station_number
  , bike_number 
from 
  dbx_arch.default.`2014_q_1_capitalbikeshare_tripdata`; 

-- COMMAND ----------

select * from dbx_arch.raws.selected_bikeshared_data();

-- COMMAND ----------

create function dbx_arch.raws.filter_duration_lt_100()
returns table 
comment 'Bikeshare data'
return 
select 
  duration
  , start_date
  , end_date
  , start_station_number
  , end_station_number
  , bike_number 
from 
  dbx_arch.default.`2014_q_1_capitalbikeshare_tripdata`
where duration < 100; 

-- COMMAND ----------

select * from dbx_arch.raws.filter_duration_lt_100(); 

-- COMMAND ----------

create or replace function dbx_arch.raws.count_bikes_from_same_start_station()
returns table
comment 'Bikeshare from same start station'
language sql
return 
select 
  start_station_number
  , count(bike_number) as count_bikes_from_same_start_station
from 
  dbx_arch.default.`2014_q_1_capitalbikeshare_tripdata` 
group by 
 start_station_number;   

-- COMMAND ----------

select * from dbx_arch.raws.count_bikes_from_same_start_station(); 

-- COMMAND ----------

create or replace function dbx_arch.raws.count_bikes_from_same_start_end_station()
returns table
comment 'Bikeshare from same start station and end in start station'
language sql
return 
select 
  start_station_number
  , count(bike_number) as count_bikes_from_same_start_station
from 
  dbx_arch.default.`2014_q_1_capitalbikeshare_tripdata` 
where 
  start_station_number = end_station_number
group by 
 start_station_number;   

-- COMMAND ----------

select * from dbx_arch.raws.count_bikes_from_same_start_end_station(); 

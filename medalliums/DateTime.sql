-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Databricks datatime convertion 
-- MAGIC ## Take the Year, Month and day from the date string 
-- MAGIC - get the year from the string 
-- MAGIC   - long form [2024]
-- MAGIC   - end of the year [24]
-- MAGIC - get the month from the sting 
-- MAGIC   - numerical [10]
-- MAGIC   - three word [oct]
-- MAGIC   - long from [october]
-- MAGIC - get the day from the string 
-- MAGIC ## Take the hour, minute and second from the HH:MM:SS string 
-- MAGIC - get hour from the string 
-- MAGIC - get minute from the string 
-- MAGIC - get second from tne string
-- MAGIC ## Take the Year, Month and day from datetime 
-- MAGIC - get the Year - month - day from the datetime 
-- MAGIC - get the hour : min : second form the datetime 
-- MAGIC ## datetime format converting [Transformation]
-- MAGIC - convert from US date format to YYYY-MM-DD
-- MAGIC - convert from UK date format to YYYY-MM-DD
-- MAGIC - convert from PASSPORT date format to YYYY-MM-DD
-- MAGIC - convert from custom Excel date from to YYYY-MM-DD 
-- MAGIC ## Adding, subtract days, month from current_date
-- MAGIC - Adding days to the current date 
-- MAGIC - Adding month to the current date 
-- MAGIC - subtraction days from the current date 
-- MAGIC - substraction days from the current date
-- MAGIC
-- MAGIC ## calculate the date diff function
-- MAGIC
-- MAGIC ## Table list
-- MAGIC | sr | SQL function | usage |
-- MAGIC |:--:|:------------:|:---------:|
-- MAGIC | 1 | from_date |  |
-- MAGIC | 2 | to_date | | 
-- MAGIC | 3 | hour | | 
-- MAGIC | 4 | minute | | 
-- MAGIC | 5 | second | | 
-- MAGIC | 6 | date_add | | 
-- MAGIC | 7 | add_month | |

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Take the Year, Month and day from the date string
-- MAGIC ### get year from date string

-- COMMAND ----------

-- Spark SQL 
-- get year from date string
-- get year 
select date_format(current_date(), 'yyyy'); 

-- COMMAND ----------

select date_format(current_date(), 'yy' )

-- COMMAND ----------

select date_format('1998-06-13', 'yyyy' )

-- COMMAND ----------

-- this kinda problem solve in next session 
select date_format('13/06/1996', 'yyyy' )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### get month from the date string

-- COMMAND ----------

select date_format(current_date(), 'MM' )

-- COMMAND ----------

select date_format(current_date(), 'MMM' )

-- COMMAND ----------

-- to get the whole month 
select date_format(current_date(), 'MMMM' )

-- COMMAND ----------

select date_format('2023-12-11', 'MM');

-- COMMAND ----------

select upper(date_format('2023-12-11', 'MMM'));

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### get date from the date string

-- COMMAND ----------

select date_format(current_date(), 'dd');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Take the hour, minute and second from the HH:MM:SS string

-- COMMAND ----------

select current_timestamp();

-- COMMAND ----------

select current_timezone();

-- COMMAND ----------

select date_format(current_timestamp(), 'HH:mm:ss')

-- COMMAND ----------

select 
  hour(current_timestamp()) as hour 
  , minute(current_timestamp()) as min
  , second(current_timestamp()) as sec
  ;


-- COMMAND ----------

select current_timestamp();

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Date converting from various format to YYYY-MM-DD format

-- COMMAND ----------

select date_format(to_date('10/10/2023', 'MM/dd/yyyy'), 'yyyy-MM-dd') as formated_date

-- COMMAND ----------

select date_format(to_date('15/Oct/2023', 'dd/MMM/yyyy'), 'yyyy-MM-dd') as formated_date

-- COMMAND ----------

select '2024' || '-' || '01' || '-' || '01' as formated_date 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Adding, subtract days, month from current_date

-- COMMAND ----------

select date_format(date_add(current_date(), 12), 'yyyy-MM-dd' ) as formated_date;

-- COMMAND ----------

select date_format(date_add(current_date(), 30), 'yyyy-MM-dd' ) as formated_date

-- COMMAND ----------

select date_format(add_months(current_date(), 2), 'yyyy-MM-dd') as formated_date

-- COMMAND ----------

select date_format(date_sub(current_date(), 2), 'yyyy-MM-dd') as formated_date

-- COMMAND ----------

-- subtracting month
select date_format(add_months(current_date(), -2), 'yyyy-MM-dd') as formated_date

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### calculate the date diff data
-- MAGIC

-- COMMAND ----------

select datediff('2024-10-10', '2024-5-10') as date_diff

-- COMMAND ----------

select months_between('2024-10-10', '2024-3-17' ) as monnt_between

-- COMMAND ----------



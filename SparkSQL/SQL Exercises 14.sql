-- Databricks notebook source
-- MAGIC %md 
-- MAGIC # Advance SQL
-- MAGIC - windows (lead and lag)
-- MAGIC - pivot 
-- MAGIC - grouping set 
-- MAGIC - rollup

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW high_temps
  USING csv
  OPTIONS (path "/databricks-datasets/weather/high_temps", header "true", mode "FAILFAST")

-- COMMAND ----------

select * from high_temps;

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW low_temps
  USING csv
  OPTIONS (path "/databricks-datasets/weather/low_temps", header "true", mode "FAILFAST")

-- COMMAND ----------

select * from (
  select 
    year(date) as year
    , month(date) as month
    , temp 
  from high_temps
  where date between date '2015-01-01' and date '2018-12-31'
  union 
  select 
    year(date) as year
    , month(date) as month
    , temp 
  from low_temps
  where date between date '2015-01-01' and date '2018-08-31'
) pivot (
  round(avg(temp), 4) for month in (
    1 Jan, 2 Feb, 3 Mar, 4 Apr,
    5 May, 6 Jun, 7 Jul, 8 Aug,
    9 Sep, 10 Oct, 11 Nov, 12 Dec)
) 
order by year

-- COMMAND ----------

select * from (
  select year(date) as year , month(date) as month , temp, flag `H/L` from (
  select 
    date
    , temp 
    , 'H' as flag 
  from high_temps
  union all
  select 
    date
    , temp 
    , 'L' as flag
  from low_temps
  )
  where date between DATE '2015-01-01' AND DATE '2018-08-31'
) pivot (
  round(avg(temp), 4) for month in (
    1 Jan, 2 Feb, 3 Mar, 4 Apr
) )
order by year, `H/L`

-- COMMAND ----------

SELECT * FROM (
  SELECT year(date) year, month(date) month, temp, flag `H/L`
  FROM (
    SELECT date, temp, 'H' as flag
    FROM high_temps
    UNION ALL
    SELECT date, temp, 'L' as flag
    FROM low_temps
  )
  WHERE date BETWEEN DATE '2015-01-01' AND DATE '2018-08-31'
)
PIVOT (
  CAST(avg(temp) AS DECIMAL(4, 1))
  FOR month in (6 JUN, 7 JUL, 8 AUG, 9 SEP)
)
ORDER BY year DESC, `H/L` ASC

-- COMMAND ----------

SELECT * FROM (
  SELECT year(date) year, month(date) month, temp, flag
  FROM (
    SELECT date, temp, 'H' as flag
    FROM high_temps
    UNION ALL
    SELECT date, temp, 'L' as flag
    FROM low_temps
  )
  WHERE date BETWEEN DATE '2015-01-01' AND DATE '2018-08-31'
)
PIVOT (
  CAST(avg(temp) AS DECIMAL(4, 1))
  FOR (month, flag) in (
    (6, 'H') JUN_hi, (6, 'L') JUN_lo,
    (7, 'H') JUL_hi, (7, 'L') JUL_lo,
    (8, 'H') AUG_hi, (8, 'L') AUG_lo,
    (9, 'H') SEP_hi, (9, 'L') SEP_lo
  )
)
ORDER BY year DESC

-- COMMAND ----------

select 
  year(date) as years,
  cast (avg(temp) as decimal(4,2)) as temp
from 
  high_temps
group by
  year(date) 
order by 
  years;

-- COMMAND ----------

CREATE TEMP VIEW dealer (id, city, car_model, quantity) AS
VALUES (100, 'Fremont', 'Honda Civic', 10),
       (100, 'Fremont', 'Honda Accord', 15),
       (100, 'Fremont', 'Honda CRV', 7),
       (200, 'Dublin', 'Honda Civic', 20),
       (200, 'Dublin', 'Honda Accord', 10),
       (200, 'Dublin', 'Honda CRV', 3),
       (300, 'San Jose', 'Honda Civic', 5),
       (300, 'San Jose', 'Honda Accord', 8);

-- COMMAND ----------

select * from dealer;

-- COMMAND ----------

select city, sum(quantity) from dealer group by city

-- COMMAND ----------

select city, sum(quantity) as total, max(quantity) as max from dealer group by city

-- COMMAND ----------

SELECT id,
       sum(quantity) FILTER (WHERE car_model IN ('Honda Civic', 'Honda CRV')) AS `sum(quantity)`
FROM dealer
GROUP BY id 
ORDER BY id;

-- COMMAND ----------

SELECT city, car_model, sum(quantity) AS sum
    FROM dealer
    GROUP BY GROUPING SETS ((city, car_model), (city), (car_model), ())
    ORDER BY city;

-- COMMAND ----------

SELECT city, car_model, sum(quantity) AS sum
    FROM dealer
    GROUP BY city, car_model WITH ROLLUP
    ORDER BY city, car_model;


-- COMMAND ----------

SELECT city, car_model, sum(quantity) AS sum
    FROM dealer
    GROUP BY city, car_model WITH CUBE
    ORDER BY city, car_model;

-- COMMAND ----------

SELECT
    city,
    car_model,
    RANK() OVER (PARTITION BY car_model ORDER BY quantity) AS rank
  FROM dealer
  QUALIFY rank = 1;

-- COMMAND ----------

CREATE TEMP VIEW person (name, age)
    AS VALUES ('Zen Hui', 25),
              ('Anil B', 18),
              ('Shone S', 16),
              ('Mike A', 25),
              ('John A', 18),
              ('Jack N', 16);

-- COMMAND ----------

select * from person cluster by age;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.conf.get("spark.sql.shuffle.partitions")

-- COMMAND ----------

SET spark.sql.shuffle.partitions;

-- COMMAND ----------



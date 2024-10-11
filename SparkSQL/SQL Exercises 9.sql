-- Databricks notebook source
-- MAGIC %md
-- MAGIC ###  Determine the Most Popular Google Search Category
-- MAGIC https://datalemur.com/blog/google-sql-interview-questions

-- COMMAND ----------

use catalog dbx_sthz; 
use schema db_demo;

-- COMMAND ----------

create table if not exists searches (
  search_id	int,
  user_id	int,
  search_date timestamp,
  category_id int,
  query varchar(30)
);
--
create table if not exists categories (
  category_id int, 
  category_name varchar(30)
);

-- COMMAND ----------

insert into searches values 
(1001,	7654,	'2024-06-01 00:00:00',	3001,	"chicken recipe"),
(1002,	2346,	'2024-06-02 00:00:00',	3001,	"vegan meal prep"),
(1003,	8765,	'2024-06-03 00:00:00',	2001,	"google stocks"),
(1004,	9871,	'2024-07-01 00:00:00',	1001,	"python tutorial"),
(1005,	8760,	'2024-07-02 00:00:00',	2001,	"tesla stocks"); 

-- 
insert into categories values 
(1001,	"Programming Tutorials"),
(2001,	"Stock Market"),
(3001,	"Recipes"),
(4001,	"Sports News"); 

-- COMMAND ----------

select 
  category_name,
  month(search_date) as search_month,
  year(search_date) || '-' || month(search_date) as year_month 
from 
  searches s 
inner join 
  categories c 
on s.category_id = c.category_id

-- COMMAND ----------

with cte_seraches_categeory_per_year_month as (
select 
  category_name,
  month(search_date) as search_month,
  year(search_date) || '-' || month(search_date) as year_month 
from 
  searches s 
inner join 
  categories c 
on s.category_id = c.category_id
)
select 
  category_name, 
  substr(year_month, 6,7) as search_month,
  count(category_name) as search_count
from 
  cte_seraches_categeory_per_year_month
group by category_name, year_month
order by search_count asc; 


-- COMMAND ----------

select 
*
from 
  searches s 
inner join 
  categories c 
on s.category_id = c.category_id

-- COMMAND ----------

select 
  category_name,
  month(search_date) as search_month,
  count(*) over(partition by c.category_name order by month(search_date)) as search_count
from 
  searches s 
left join 
  categories c 
on s.category_id = c.category_id
where 
  year(search_date) = 2024
group by c.category_name, search_month
order by
  search_count

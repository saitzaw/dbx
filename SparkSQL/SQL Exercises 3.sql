-- Databricks notebook source
use catalog dbx_sthz;
use schema db_demo; 

-- COMMAND ----------

Create table If Not Exists Employee (id int, name varchar(255), salary int, departmentId int); 
insert into Employee (id, name, salary, departmentId) values 
('1', 'Joe', '85000', '1')
, ('2', 'Henry', '80000', '2')
, ('3', 'Sam', '60000', '2')
, ('4', 'Max', '90000', '1')
, ('5', 'Janet', '69000', '1')
, ('6', 'Randy', '85000', '1')
, ('7', 'Will', '70000', '1'); 

-- COMMAND ----------


Create table If Not Exists Department (id int, name varchar(255));

insert into Department (id, name) values 
('1', 'IT')
, ('2', 'Sales');

-- COMMAND ----------

select 
  * 
from 
  Employee e
join Department d on e.departmentId = d.id;

-- COMMAND ----------

with cte_top_three_income as (
select 
  e.name as employee_name
  , d.name as deparment_name
  , e.salary 
  , row_number() over(partition by e.departmentId order by salary desc) as rn 
from 
  Employee e 
inner join Department d on e.departmentId = d.id 
qualify rn <= 3
)
select 
  employee_name
  , deparment_name
  , salary  
from cte_top_three_income; 


-- COMMAND ----------



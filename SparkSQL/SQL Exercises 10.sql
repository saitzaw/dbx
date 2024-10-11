-- Databricks notebook source
-- MAGIC %md 
-- MAGIC ### pivot table 
-- MAGIC https://builtin.com/articles/sql-pivot

-- COMMAND ----------

use catalog dbx_sthz; 
use schema db_demo; 

-- COMMAND ----------

create table if not exists FoodEaten 
(
  id int ,
  eat_date date,
  FoodName varchar(20),
  AmountEaten int); 

-- COMMAND ----------

insert into FoodEaten values 
(1,'2019-08-01','Sammich',2),
(2,'2019-08-01','Pickle',3),
(3,'2019-08-01','Apple',1),
(4,'2019-08-02','Sammich',1),
(5,'2019-08-02','Pickle',1),
(6,'2019-08-02','Apple',4),
(7,'2019-08-03','Cake',2),
(8,'2019-08-04','Sammich',1),
(9,'2019-08-04','Pickle',2),
(10,'2019-08-04','Apple',3); 

-- COMMAND ----------

select * from FoodEaten;

-- COMMAND ----------

SELECT [Date] AS 'Day',
    ISNULL([Sammich], 0) AS Sammich,
    ISNULL([Pickle],  0) AS Pickle, 
    ISNULL([Apple],   0) AS Apple,
    ISNULL([Cake],    0) AS Cake
FROM (
    SELECT [Date], FoodName, AmountEaten FROM FoodEaten
) AS SourceTable
PIVOT (
    MAX(AmountEaten)
    FOR FoodName IN (
        [Sammich], [Pickle], [Apple], [Cake]
    )
) AS PivotTable

-- COMMAND ----------

select 
  id,
  eat_date,
  coalesce(Sammich, 0) as Sammich,
  coalesce(Pickle, 0) as Pickle,
  coalesce(Apple, 0) as Apple,
  coalesce(Cake, 0) as Cake
from
(
  select 
    id,
    eat_date, 
    foodname,
    amounteaten 
  from 
    FoodEaten 
)
pivot (max(AmountEaten) for FoodName in ("Sammich", "Pickle", "Apple", "Cake"))

-- Databricks notebook source
-- MAGIC %md
-- MAGIC ###   Google Maps Flagged UGC
-- MAGIC https://datalemur.com/blog/google-sql-interview-questions
-- MAGIC

-- COMMAND ----------

use catalog dbx_sthz; 
use schema db_demo; 

-- COMMAND ----------

create table if not exists place_info (place_id int, place_name varchar(30), place_category varchar(30));
create table if not exists maps_ugc_review (content_id int,	place_id int,	content_tag varchar(30));

-- COMMAND ----------

insert into place_info values
(1,	'Baar Baar',	'Restaurant'),
(2,	'Rubirosa',	'Restaurant'),
(3,	'Mr. Purple',	'Bar'),
(4,	'La Caverna',	'Bar');

-- 
insert into maps_ugc_review values 
(101,	1,	'Off-topic'),
(110,	2,	'Misinformation'),
(153,	2,	'Off-topic'),
(176,	3,	'Harassment'),
(190,	3,	'Off-topic');


-- COMMAND ----------

WITH ContentCounts AS (
    SELECT 
      p.place_category,
      count(m.content_id) as content_count
    FROM 
        place_info p 
    JOIN 
        maps_ugc_review m 
    ON 
        p.place_id = m.place_id 
    WHERE lower(m.content_tag) = 'off-topic'
    GROUP BY p.place_category
), RankedContentCounts AS (
  SELECT 
    place_category,
    RANK() OVER (ORDER BY content_count DESC) as rn 
  FROM 
      ContentCounts
  QUALIFY rn = 1
)
SELECT place_category as top_category FROM RankedContentCounts
;

-- COMMAND ----------



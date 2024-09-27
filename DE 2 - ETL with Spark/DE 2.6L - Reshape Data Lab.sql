-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
-- MAGIC </div>
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC # Reshaping Data Lab
-- MAGIC
-- MAGIC In this lab, you will create a **`clickpaths`** table that aggregates the number of times each user took a particular action in **`events`** and then join this information with a flattened view of **`transactions`** to create a record of each user's actions and final purchases.
-- MAGIC
-- MAGIC The **`clickpaths`** table should contain all the fields from **`transactions`**, as well as a count of every **`event_name`** from **`events`** in its own column. This table should contain a single row for each user that completed a purchase.
-- MAGIC
-- MAGIC ## Learning Objectives
-- MAGIC By the end of this lab, you should be able to:
-- MAGIC - Pivot and join tables to create clickpaths for each user

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Run Setup
-- MAGIC
-- MAGIC The setup script will create the data and declare necessary values for the rest of this notebook to execute.

-- COMMAND ----------

-- MAGIC %run ./Includes/Classroom-Setup-02.6L

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC We'll use Python to run checks occasionally throughout the lab. The helper functions below will return an error with a message on what needs to change if you have not followed instructions. No output means that you have completed this step.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC def check_table_results(table_name, num_rows, column_names):
-- MAGIC     assert spark.table(table_name), f"Table named **`{table_name}`** does not exist"
-- MAGIC     assert set(spark.table(table_name).columns) == set(column_names), "Please name the columns as shown in the schema above"
-- MAGIC     assert spark.table(table_name).count() == num_rows, f"The table should have {num_rows} records"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Pivot events to get event counts for each user
-- MAGIC
-- MAGIC Let's start by pivoting the **`events`** table to get counts for each **`event_name`**.
-- MAGIC
-- MAGIC We want to aggregate the number of times each user performed a specific event, specified in the **`event_name`** column. To do this, group by **`user_id`** and pivot on **`event_name`** to provide a count of every event type in its own column, resulting in the schema below. Note that **`user_id`** is renamed to **`user`** in the target schema.
-- MAGIC
-- MAGIC | field | type | 
-- MAGIC | --- | --- | 
-- MAGIC | user | STRING |
-- MAGIC | cart | BIGINT |
-- MAGIC | pillows | BIGINT |
-- MAGIC | login | BIGINT |
-- MAGIC | main | BIGINT |
-- MAGIC | careers | BIGINT |
-- MAGIC | guest | BIGINT |
-- MAGIC | faq | BIGINT |
-- MAGIC | down | BIGINT |
-- MAGIC | warranty | BIGINT |
-- MAGIC | finalize | BIGINT |
-- MAGIC | register | BIGINT |
-- MAGIC | shipping_info | BIGINT |
-- MAGIC | checkout | BIGINT |
-- MAGIC | mattresses | BIGINT |
-- MAGIC | add_item | BIGINT |
-- MAGIC | press | BIGINT |
-- MAGIC | email_coupon | BIGINT |
-- MAGIC | cc_info | BIGINT |
-- MAGIC | foam | BIGINT |
-- MAGIC | reviews | BIGINT |
-- MAGIC | original | BIGINT |
-- MAGIC | delivery | BIGINT |
-- MAGIC | premium | BIGINT |
-- MAGIC
-- MAGIC A list of the event names are provided in the TODO cells below.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### Solve with SQL

-- COMMAND ----------

select * from events limit 1;

-- COMMAND ----------

create or replace temp view events_pivot as (
select 
  string(user_id) as user
  , bigint(count("cart")) as cart
  , bigint(count("pillows")) as pillows 
  , bigint(count("login")) as login
  , bigint(count("main")) as main
  , bigint(count("careers")) as careers 
  , bigint(count("guest")) as guest
  , bigint(count("faq")) as faq
  , bigint(count("down")) as down
  , bigint(count("warranty")) as warranty
  , bigint(count("finalize")) as finalize
  , bigint(count("register")) as register
  , bigint(count("shipping_info")) as shipping_info
  , bigint(count("checkout")) as checkout
  , bigint(count("mattresses")) as mattresses
  , bigint(count("add_item")) as add_item
  , bigint(count("press")) as press
  , bigint(count("email_coupon")) as email_coupon
  , bigint(count("cc_info")) as cc_info
  , bigint(count("foam")) as foam
  , bigint(count("reviews")) as reviews
  , bigint(count("original")) as original
  , bigint(count("delivery")) as delivery
  , bigint(count("premium")) as premium
from (
  select 
    * 
  from events 
  pivot(
    count(event_name) for event_name in (
      "cart"
      , "pillows"
      , "login"
      , "main"
      , "careers"
      , "guest"
      , "faq"
      , "down"
      , "warranty"
      , "finalize"
      , "register"
      , "shipping_info"
      , "checkout"
      , "mattresses"
      , "add_item"
      , "press"
      , "email_coupon"
      , "cc_info"
      , "foam"
      , "reviews"
      , "original"
      , "delivery"
      , "premium"
      )
    )
)
group by 1
)

-- COMMAND ----------

select count(*) from events_pivot;

-- COMMAND ----------

 create or replace temp view events_pivot2 as (
 select 
    * 
  from events 
  pivot(
    sum(user_id) for event_name in (
      "cart"
      , "pillows"
      , "login"
      , "main"
      , "careers"
      , "guest"
      , "faq"
      , "down"
      , "warranty"
      , "finalize"
      , "register"
      , "shipping_info"
      , "checkout"
      , "mattresses"
      , "add_item"
      , "press"
      , "email_coupon"
      , "cc_info"
      , "foam"
      , "reviews"
      , "original"
      , "delivery"
      , "premium"
      )
    )
 )

-- COMMAND ----------

select count(*) from events_pivot2;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC set(spark.table("events_pivot").columns)

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC set(["cart", "pillows", "login", "main", "careers", "guest", "faq", "down", "warranty", "finalize", 
-- MAGIC "register", "shipping_info", "checkout", "mattresses", "add_item", "press", "email_coupon", 
-- MAGIC "cc_info", "foam", "reviews", "original", "delivery", "premium"
-- MAGIC ])

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC set(spark.table("events_pivot").columns) == set(["cart", "pillows", "login", "main", "careers", "guest", "faq", "down", "warranty", "finalize", 
-- MAGIC "register", "shipping_info", "checkout", "mattresses", "add_item", "press", "email_coupon", 
-- MAGIC "cc_info", "foam", "reviews", "original", "delivery", "premium"
-- MAGIC ])

-- COMMAND ----------

SELECT * FROM events_pivot LIMIT 10;

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC spark.sql("""select * from events limit 1;""").show(1,truncate=False, vertical=True)

-- COMMAND ----------

-- CREATE OR REPLACE TEMP VIEW events_pivot
-- <FILL_IN>
-- ("cart", "pillows", "login", "main", "careers", "guest", "faq", "down", "warranty", "finalize", 
-- "register", "shipping_info", "checkout", "mattresses", "add_item", "press", "email_coupon", 
-- "cc_info", "foam", "reviews", "original", "delivery", "premium")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # SOURCE_ONLY
-- MAGIC # for testing only; include checks after each language solution
-- MAGIC check_table_results("events_pivot", 204586, ['user', 'cart', 'pillows', 'login', 'main', 'careers', 'guest', 'faq', 'down', 'warranty', 'finalize', 'register', 'shipping_info', 'checkout', 'mattresses', 'add_item', 'press', 'email_coupon', 'cc_info', 'foam', 'reviews', 'original', 'delivery', 'premium'])

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### Solve with Python

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # TODO
-- MAGIC # (spark.read
-- MAGIC #     <FILL_IN>
-- MAGIC #     .createOrReplaceTempView("events_pivot"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### Check your work
-- MAGIC Run the cell below to confirm the view was created correctly.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC check_table_results("events_pivot", 204586, ['user', 'cart', 'pillows', 'login', 'main', 'careers', 'guest', 'faq', 'down', 'warranty', 'finalize', 'register', 'shipping_info', 'checkout', 'mattresses', 'add_item', 'press', 'email_coupon', 'cc_info', 'foam', 'reviews', 'original', 'delivery', 'premium'])

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC ## Join event counts and transactions for all users
-- MAGIC
-- MAGIC Next, join **`events_pivot`** with **`transactions`** to create the table **`clickpaths`**. This table should have the same event name columns from the **`events_pivot`** table created above, followed by columns from the **`transactions`** table, as shown below.
-- MAGIC
-- MAGIC | field | type | 
-- MAGIC | --- | --- | 
-- MAGIC | user | STRING |
-- MAGIC | cart | BIGINT |
-- MAGIC | ... | ... |
-- MAGIC | user_id | STRING |
-- MAGIC | order_id | BIGINT |
-- MAGIC | transaction_timestamp | BIGINT |
-- MAGIC | total_item_quantity | BIGINT |
-- MAGIC | purchase_revenue_in_usd | DOUBLE |
-- MAGIC | unique_items | BIGINT |
-- MAGIC | P_FOAM_K | BIGINT |
-- MAGIC | M_STAN_Q | BIGINT |
-- MAGIC | P_FOAM_S | BIGINT |
-- MAGIC | M_PREM_Q | BIGINT |
-- MAGIC | M_STAN_F | BIGINT |
-- MAGIC | M_STAN_T | BIGINT |
-- MAGIC | M_PREM_K | BIGINT |
-- MAGIC | M_PREM_F | BIGINT |
-- MAGIC | M_STAN_K | BIGINT |
-- MAGIC | M_PREM_T | BIGINT |
-- MAGIC | P_DOWN_S | BIGINT |
-- MAGIC | P_DOWN_K | BIGINT |

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### Solve with SQL

-- COMMAND ----------

select * from transactions limit 10; 

-- COMMAND ----------

select * from events_pivot ep join transactions t on ep.user = t.user_id;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW clickpaths AS(
select 
  ep.user
  , ep.cart
  , ep.pillows
  , ep.login
  , ep.main
  , ep.careers
  , ep.guest
  , ep.faq
  , ep.down
  , ep.warranty
  , ep.finalize
  , ep.register
  , ep.shipping_info
  , ep.checkout
  , ep.mattresses
  , ep.add_item
  , ep.press
  , ep.email_coupon
  , ep.cc_info
  , ep.foam
  , ep.reviews
  , ep.original
  , ep.delivery
  , ep.premium
  , t.user_id
  , t.order_id
  , t.transaction_timestamp
  , t.total_item_quantity
  , t.purchase_revenue_in_usd
  , t.unique_items
  , t.P_FOAM_K
  , t.M_STAN_Q
  , t.P_FOAM_S
  , t.M_PREM_Q
  , t.M_STAN_F
  , t.M_STAN_T
  , t.M_PREM_K
  , t.M_PREM_F
  , t.M_STAN_K
  , t.M_PREM_T
  , t.P_DOWN_S
  , t.P_DOWN_K
 from 
  events_pivot ep 
 join 
  transactions t 
 on ep.user = t.user_id
);


-- COMMAND ----------

-- MAGIC %python
-- MAGIC # SOURCE_ONLY
-- MAGIC check_table_results("clickpaths", 9085, ['user', 'cart', 'pillows', 'login', 'main', 'careers', 'guest', 'faq', 'down', 'warranty', 'finalize', 'register', 'shipping_info', 'checkout', 'mattresses', 'add_item', 'press', 'email_coupon', 'cc_info', 'foam', 'reviews', 'original', 'delivery', 'premium', 'user_id', 'order_id', 'transaction_timestamp', 'total_item_quantity', 'purchase_revenue_in_usd', 'unique_items', 'P_FOAM_K', 'M_STAN_Q', 'P_FOAM_S', 'M_PREM_Q', 'M_STAN_F', 'M_STAN_T', 'M_PREM_K', 'M_PREM_F', 'M_STAN_K', 'M_PREM_T', 'P_DOWN_S', 'P_DOWN_K'])

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### Solve with Python

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # TODO
-- MAGIC (spark.read
-- MAGIC     .table("clickpaths")
-- MAGIC     .createOrReplaceTempView("clickpaths"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### Check your work
-- MAGIC Run the cell below to confirm the view was created correctly.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC check_table_results("clickpaths", 9085, ['user', 'cart', 'pillows', 'login', 'main', 'careers', 'guest', 'faq', 'down', 'warranty', 'finalize', 'register', 'shipping_info', 'checkout', 'mattresses', 'add_item', 'press', 'email_coupon', 'cc_info', 'foam', 'reviews', 'original', 'delivery', 'premium', 'user_id', 'order_id', 'transaction_timestamp', 'total_item_quantity', 'purchase_revenue_in_usd', 'unique_items', 'P_FOAM_K', 'M_STAN_Q', 'P_FOAM_S', 'M_PREM_Q', 'M_STAN_F', 'M_STAN_T', 'M_PREM_K', 'M_PREM_F', 'M_STAN_K', 'M_PREM_T', 'P_DOWN_S', 'P_DOWN_K'])

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC  
-- MAGIC Run the following cell to delete the tables and files associated with this lesson.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DA.cleanup()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC &copy; 2024 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the 
-- MAGIC <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/><a href="https://databricks.com/privacy-policy">Privacy Policy</a> | 
-- MAGIC <a href="https://databricks.com/terms-of-use">Terms of Use</a> | 
-- MAGIC <a href="https://help.databricks.com/">Support</a>

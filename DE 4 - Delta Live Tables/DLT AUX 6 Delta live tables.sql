-- Databricks notebook source
-- DBTITLE 1,Delta Live Tables
-- MAGIC %md
-- MAGIC 1. Create Bronze table for streaming sales order data
-- MAGIC 1. Create Bronze table for streaming customers data
-- MAGIC 1. Create Silver table by joining sales and customer data
-- MAGIC 1. Create Gold table by applying the aggregation

-- COMMAND ----------

--Create Bronze DLT - streaming live table using autoloader
CREATE OR REFRESH STREAMING LIVE TABLE sales_orders_bronze
COMMENT "The raw sales orders, ingested from retail-org/sales_orders."
AS SELECT * FROM cloud_files("/databricks-datasets/retail-org/sales_orders/", "json", map("cloudFiles.inferColumnTypes", "true"))

-- COMMAND ----------

--Create customers_master table - streaming table using autoloader
CREATE OR REFRESH STREAMING LIVE TABLE customers_bronze
COMMENT "The customers buying finished products, ingested from retail-org/customers."
AS SELECT * FROM cloud_files("/databricks-datasets/retail-org/customers/", "csv")

-- COMMAND ----------

--Create Silver DLT
CREATE OR REFRESH STREAMING LIVE TABLE sales_orders_silver(
  CONSTRAINT valid_order_number EXPECT (order_number IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT "The cleaned sales orders with valid order_number(s)."
AS
  SELECT 
    f.customer_id
    , f.customer_name
    , f.number_of_line_items
    , timestamp(from_unixtime((cast(f.order_datetime as long)))) as order_datetime
    , date(from_unixtime((cast(f.order_datetime as long)))) as order_date
    , f.order_number
    , f.ordered_products
    , c.state
    , c.city
    , c.lon
    , c.lat
    , c.units_purchased
    , c.loyalty_segment
  FROM STREAM(LIVE.sales_orders_bronze) f
  LEFT JOIN LIVE.customers_bronze c
    ON c.customer_id = f.customer_id
    AND c.customer_name = f.customer_name

-- COMMAND ----------

--Create Gold DLT - materialized view
CREATE OR REFRESH LIVE TABLE sales_order_in_ny_gold
COMMENT "Sales orders in LA."
AS
  SELECT city, order_date, customer_id, customer_name, ordered_products_explode.curr, 
         sum(ordered_products_explode.price) as sales, 
         sum(ordered_products_explode.qty) as quantity, 
         count(ordered_products_explode.id) as product_count
  FROM (SELECT city, order_date, customer_id, customer_name, explode(ordered_products) as ordered_products_explode
        FROM LIVE.sales_orders_silver 
        WHERE state = 'NY')
  GROUP BY order_date, city, customer_id, customer_name, ordered_products_explode.curr

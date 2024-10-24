-- Databricks notebook source
-- MAGIC %python
-- MAGIC # Python code to set the catalog name
-- MAGIC catalog_name = "dbx_arch"
-- MAGIC schema_name = "data_vault2"
-- MAGIC # SQL query to use the catalog
-- MAGIC spark.sql(f"USE CATALOG {catalog_name}")
-- MAGIC spark.sql(f"USE SCHEMA {schema_name}")

-- COMMAND ----------

-- select current_catalog();

-- COMMAND ----------

-- select current_schema()

-- COMMAND ----------



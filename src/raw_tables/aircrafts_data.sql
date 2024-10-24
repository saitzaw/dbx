-- Databricks notebook source
-- MAGIC %run ../macros/uc_catalog_config

-- COMMAND ----------



-- COMMAND ----------

-- data profiling here
select * from `landing_zone`.aircrafts_data


-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.

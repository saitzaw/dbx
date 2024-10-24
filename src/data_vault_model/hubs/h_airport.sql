-- Databricks notebook source
-- MAGIC %run ../../macros/uc_catalog_config

-- COMMAND ----------

create table h_airport (
	 hk_airport   char(32)  not null
	,airport_code char(3)   not null
	,load_date_ts timestamp not null
	,constraint h_airport_pk
	 primary key (hk_airport)
);

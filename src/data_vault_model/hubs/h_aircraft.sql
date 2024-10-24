-- Databricks notebook source
-- MAGIC %run ../../macros/uc_catalog_config

-- COMMAND ----------

create table h_aircraft (
	 hk_aircraft    char(32)  not null
	,aircraft_code  char(3)   not null
	,load_date_ts   timestamp not null
	,constraint h_aircraft_pk
		primary key (hk_aircraft)
);

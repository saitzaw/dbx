-- Databricks notebook source
-- MAGIC %run ../../macros/uc_catalog_config

-- COMMAND ----------

create table s_airport (
	 hk_airport   char(32)  not null
	,load_date_ts timestamp not null
	,airport_name string      not null 
	,city         string      not null
	,coordinates  string      not null
	,timezone     string      not null
	,constraint s_airport_pk 
		primary key (hk_airport, load_date_ts)
	,constraint s_airport_h_airport_fk
		foreign key (hk_airport)
		references h_airport(hk_airport)
);

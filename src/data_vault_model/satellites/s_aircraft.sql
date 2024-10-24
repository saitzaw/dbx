-- Databricks notebook source
-- MAGIC %run ../../macros/uc_catalog_config

-- COMMAND ----------

create table s_aircraft (
	 hk_aircraft  char(32)    not null
	,load_date_ts timestamp   not null
	,model        varchar(25) not null
	,range integer
	,constraint s_aircraft_pk 
		primary key (hk_aircraft, load_date_ts)
	,constraint s_aircraft_h_aircraft_fk
		foreign key (hk_aircraft)
		references h_aircraft(hk_aircraft)
);

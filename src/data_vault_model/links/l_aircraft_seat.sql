-- Databricks notebook source
-- MAGIC %run ../../macros/uc_catalog_config

-- COMMAND ----------

create table l_aircraft_seat (
	 lhk_aircraft_seat char(32)    not null
	,hk_aircraft       char(32)    not null
	,hk_seat           char(32)    not null
	,load_date_ts      timestamp   not null
	,constraint l_aircraft_seat_pk
		primary key (lhk_aircraft_seat)
	,constraint l_aircraft_seat_h_aircraft_fk
		foreign key (hk_aircraft) 
		references h_aircraft (hk_aircraft)
	,constraint l_aircraft_seat_h_seat_fk
		foreign key (hk_seat)
		references h_seat (hk_seat)
);


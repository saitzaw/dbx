-- Databricks notebook source
-- MAGIC %run ../../macros/uc_catalog_config

-- COMMAND ----------

-- Seat-Satellite
create table s_seat (
	 hk_seat         char(32)    not null
	,load_date_ts    timestamp   not null
	,fare_conditions varchar(10) not null
	,constraint s_seat_pk
		primary key (hk_seat, load_date_ts)
	,constraint s_seat_h_seat_fk
		foreign key (hk_seat)
		references h_seat(hk_seat)
);

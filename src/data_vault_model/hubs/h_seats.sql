-- Databricks notebook source
-- MAGIC %run ../../macros/uc_catalog_config

-- COMMAND ----------

create table h_seat (
	 hk_seat        char(32)    not null
	,aircraft_code  char(3)     not null
	,seat_no        varchar(4)  not null
	,load_date_ts   timestamp   not null
	,constraint h_seat_pk 
		primary key (hk_seat)
);

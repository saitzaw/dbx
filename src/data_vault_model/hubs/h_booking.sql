-- Databricks notebook source
-- MAGIC %run ../../macros/uc_catalog_config

-- COMMAND ----------

create table h_booking (
	 hk_booking    char(32)  not null
	,book_ref      char(6)   not null
	,load_date_ts timestamp  not null
	,constraint h_booking_pk 
		primary key (hk_booking)
);

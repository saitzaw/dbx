-- Databricks notebook source
-- MAGIC %run ../../macros/uc_catalog_config

-- COMMAND ----------

create table s_booking (
	hk_booking    char(32)       not null
	,load_date_ts timestamp      not null
	,book_date    date           not null
	,total_amount numeric(10, 2) not null
	,constraint s_booking_pk
		primary key (hk_booking, load_date_ts)
	,constraint s_booking_h_booking_fk
		foreign key (hk_booking)
		references h_booking(hk_booking)
);

-- COMMAND ----------



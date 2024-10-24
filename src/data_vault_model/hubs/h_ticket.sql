-- Databricks notebook source
-- MAGIC %run ../../macros/uc_catalog_config

-- COMMAND ----------

create table h_ticket (
	 hk_ticket    char(32)  not null
	,ticket_no    char(13)  not null
	,load_date_ts timestamp not null
	,constraint h_ticket_pk 
		primary key (hk_ticket)
);

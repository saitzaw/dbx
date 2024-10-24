-- Databricks notebook source
-- MAGIC %run ../../macros/uc_catalog_config

-- COMMAND ----------

create table s_ticket (
	 hk_ticket       char(32)      not null
	,load_date_ts    timestamp     not null
	,passenger_id    varchar(20)   not null
	,passenger_name  string        not null
	,passenger_phone string
	,passenger_email string
	,constraint s_ticket_pk
		primary key (hk_ticket, load_date_ts)
	,constraint s_ticket_h_ticket_fk
		foreign key (hk_ticket)
		references h_ticket(hk_ticket)
);

-- Databricks notebook source
-- MAGIC %run ../../macros/uc_catalog_config

-- COMMAND ----------

create table l_booking_ticket (
	lhk_booking_ticket char(32)  not null
	,hk_booking        char(32)  not null
	,hk_ticket         char(32)  not null
	,load_date_ts      timestamp not null
	,constraint l_booking_ticket_pk 
		primary key (lhk_booking_ticket)
	,constraint l_booking_ticket_h_booking_fk
		foreign key (hk_booking)
		references h_booking(hk_booking)
	,constraint l_booking_ticket_h_ticket_fk
		foreign key (hk_ticket)
		references h_ticket(hk_ticket)
);

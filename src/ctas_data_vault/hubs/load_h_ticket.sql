-- Databricks notebook source
insert into dbx_arch.data_vault2.h_ticket (
	hk_ticket
	,ticket_no
	,load_date_ts
) select 
	 md5(upper(trim(ticket_no)))
	,ticket_no
	,current_timestamp
   from dbx_arch.landing_zone.tickets;


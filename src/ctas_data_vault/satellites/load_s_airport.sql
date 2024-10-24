-- Databricks notebook source
 insert into dbx_arch.data_vault2.s_airport (
 	hk_airport
	,load_date_ts
	,airport_name 
	,city
	,coordinates
	,timezone
) select 
	 md5(upper(trim(airport_code)))
	,current_timestamp
	,airport_name 
	,city
	,coordinates
	,timezone
 from dbx_arch.landing_zone.airports_data;

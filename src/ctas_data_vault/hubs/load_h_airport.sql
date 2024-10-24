-- Databricks notebook source
insert into dbx_arch.data_vault2.h_airport (
	 hk_airport
	,airport_code
	,load_date_ts
) select 
	md5(upper(trim(airport_code)))
	,airport_code
	,current_timestamp
  from dbx_arch.landing_zone.airports_data;

-- Databricks notebook source
 insert into `dbx_arch`.`data_vault2`.s_aircraft (
 	 hk_aircraft
	,load_date_ts
	,model
	,range
 ) select 
 	 md5(upper(trim(aircraft_code)))
 	,current_timestamp
 	,get_json_object(model, '$.en') as model
 	,range
 from `dbx_arch`.`landing_zone`.aircrafts_data;

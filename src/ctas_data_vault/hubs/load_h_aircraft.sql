-- Databricks notebook source
-- Aircarft-Hub
insert into `dbx_arch`.`data_vault2`.h_aircraft (
	 hk_aircraft
	,aircraft_code
	,load_date_ts
) select 
	 md5(upper(trim(aircraft_code)))
	,aircraft_code 
	,current_timestamp
  from `dbx_arch`.`landing_zone`.aircrafts_data;

-- Databricks notebook source
insert into dbx_arch.data_vault2.l_aircraft_seat (
 	lhk_aircraft_seat
	,hk_aircraft
	,hk_seat
	,load_date_ts
 ) select 
 	md5(
 		upper(trim(aircraft_code))
 		|| '|'
 		|| upper(trim(aircraft_code))
 		|| '|'
 		|| upper(trim(seat_no))
 	)
 	,md5(upper(trim(aircraft_code)))
 	,md5(
 		upper(trim(aircraft_code)) 
 	 	|| '|'
 	 	|| upper(trim(seat_no))
 	 )
 	,current_timestamp
  from dbx_arch.landing_zone.seats; 

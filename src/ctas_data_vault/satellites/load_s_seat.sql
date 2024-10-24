-- Databricks notebook source
 insert into dbx_arch.data_vault2.s_seat (
 	 hk_seat
	,load_date_ts
	,fare_conditions
 ) select
 	md5(
 		upper(trim(aircraft_code)) 
 	 	|| '|'
 	 	|| upper(trim(seat_no))
 	 )
 	,current_timestamp
 	,fare_conditions 
  from dbx_arch.landing_zone.seats;

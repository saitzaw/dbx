-- Databricks notebook source
insert into dbx_arch.data_vault2.h_seat (
 	 hk_seat
	,aircraft_code
	,seat_no
	,load_date_ts
 ) select 
 	 md5(
 	 	upper(trim(aircraft_code)) 
 	 	|| '|'
 	 	|| upper(trim(seat_no))
 	 )
 	,aircraft_code
 	,seat_no 
 	,current_timestamp
   from dbx_arch.landing_zone.seats;

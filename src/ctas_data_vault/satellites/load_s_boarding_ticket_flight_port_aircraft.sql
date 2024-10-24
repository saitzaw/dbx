-- Databricks notebook source
 insert into dbx_arch.data_vault2.s_boarding_ticket_flight_port_aircraft (
 	lhk_ticket_flight_airport_aircraft
 	,load_date_ts
 	,boarding_no
 	,seat_no 
 ) select
 	md5(concat(
 		upper(trim(cast(f.flight_id as varchar(255)))) 
		, '|'
		, upper(trim(f.departure_airport))
		, '|'
		, upper(trim(f.arrival_airport))
		, '|'
		, upper(trim(f.aircraft_code))
		, '|'
		, upper(trim(bp.ticket_no))
	))
	,current_timestamp()
	,bp.boarding_no 
	,bp.seat_no 
  from dbx_arch.landing_zone.flights f
  join dbx_arch.landing_zone.boarding_passes bp 
  	on f.flight_id = bp.flight_id; 

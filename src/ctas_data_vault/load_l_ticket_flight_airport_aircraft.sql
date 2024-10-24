-- Databricks notebook source
 insert into dbx_arch.data_vault2.l_ticket_flight_airport_aircraft (
 	lhk_ticket_flight_airport_aircraft
 	,hk_ticket
 	,lhk_flight_airport_aircraft
 	,load_date_ts
 ) select
 	md5(
		concat(upper(trim(cast(f.flight_id as varchar(255))))
		, '|'
		, upper(trim(f.departure_airport))
		, '|'
		, upper(trim(f.arrival_airport))
		, '|'
		, upper(trim(f.aircraft_code))
		, '|'
		, upper(trim(tf.ticket_no))
	))
	,md5(upper(trim(tf.ticket_no)))
	,md5(concat(
		upper(trim(cast(f.flight_id as varchar(255))))
		, '|'
		, upper(trim(f.departure_airport))
		, '|'
		, upper(trim(f.arrival_airport))
		, '|'
		, upper(trim(f.aircraft_code))
	))
	,current_timestamp()
  from dbx_arch.landing_zone.flights f
  join dbx_arch.landing_zone.ticket_flights tf 
  	on f.flight_id = tf.flight_id;

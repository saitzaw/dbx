-- Databricks notebook source
-- s_ticket_flight_airport_aircraft
insert into dbx_arch.data_vault2.s_ticket_flight_airport_aircraft (
	lhk_ticket_flight_airport_aircraft
	,load_date_ts
	,fare_conditions
	,amount
) select 
	md5(
    concat(
		upper(trim(cast(f.flight_id as varchar(255))))
		, '|'
		, upper(trim(f.departure_airport))
		, '|'
		, upper(trim(f.arrival_airport))
		, '|'
		, upper(trim(f.aircraft_code))
		, '|'
		, upper(trim(tf.ticket_no))
	))
	,current_timestamp()
	,tf.fare_conditions
	,tf.amount 
 from dbx_arch.landing_zone.flights f
 join dbx_arch.landing_zone.ticket_flights tf 
  	on f.flight_id = tf.flight_id;

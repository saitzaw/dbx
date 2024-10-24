-- Databricks notebook source
insert into dbx_arch.data_vault2.s_flight (
	 lhk_flight_airport_aircraft
	,load_date_ts
	,flight_no
	,scheduled_departure
	,scheduled_arrival
	,actual_departure
	,actual_arrival
	,status    
) select 
	md5(
		concat(upper(trim(cast(flight_id as varchar(255))))
		, '|'
		, upper(trim(departure_airport))
		, '|'
		, upper(trim(arrival_airport))
		, '|'
		, upper(trim(aircraft_code))
	))
	,current_timestamp
	,flight_no
	,scheduled_departure
	,scheduled_arrival
	,actual_departure
	,actual_arrival
	,status
 from dbx_arch.landing_zone.flights;

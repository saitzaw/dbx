-- Databricks notebook source
insert into dbx_arch.data_vault2.l_flight_airport_aircraft (
	lhk_flight_airport_aircraft
	,hk_departure_airport
	,hk_arrival_airport
	,hk_aircraft
	,flight_id
	,load_date_ts    
) select 
	md5(concat(
		upper(trim(cast(flight_id as varchar(255))))
		, '|'
		, upper(trim(departure_airport))
		, '|'
		, upper(trim(arrival_airport))
		, '|'
		, upper(trim(aircraft_code))
	))
	,md5(upper(trim(departure_airport)))
	,md5(upper(trim(arrival_airport)))
	,md5(upper(trim(aircraft_code)))
	,flight_id
	,current_timestamp
 from dbx_arch.landing_zone.flights;

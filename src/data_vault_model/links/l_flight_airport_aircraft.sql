-- Databricks notebook source
-- MAGIC %run ../../macros/uc_catalog_config

-- COMMAND ----------

create table l_flight_airport_aircraft (
	 lhk_flight_airport_aircraft char(32)  not null
	,hk_departure_airport        char(32)  not null
	,hk_arrival_airport          char(32)  not null
	,hk_aircraft                 char(32)  not null
	,flight_id                   integer   not null
	,load_date_ts                timestamp not null
	,constraint l_flight_airport_aircraft_pk
		primary key (lhk_flight_airport_aircraft)
	,constraint l_flight_airport_aircraft_h_departure_fk
		foreign key (hk_departure_airport)
		references h_airport(hk_airport)
	,constraint l_flight_airport_aircraft_h_arrival_fk
		foreign key (hk_arrival_airport)
		references h_airport(hk_airport)
	,constraint l_flight_airport_aircraft_h_aircraft_fk
		foreign key (hk_aircraft)
		references h_aircraft(hk_aircraft)
);

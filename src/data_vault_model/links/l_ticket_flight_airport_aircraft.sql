-- Databricks notebook source
-- MAGIC %run ../../macros/uc_catalog_config

-- COMMAND ----------

create table l_ticket_flight_airport_aircraft (
	lhk_ticket_flight_airport_aircraft char(32)  not null
	,hk_ticket                         char(32)  not null
	,lhk_flight_airport_aircraft       char(32)  not null
	,load_date_ts                      timestamp not null
	,constraint l_ticket_flight_airport_aircraft_pk
		primary key (lhk_ticket_flight_airport_aircraft)
	,constraint l_ticket_flight_airport_aircraft_h_ticket_fk
		foreign key (hk_ticket)
		references h_ticket (hk_ticket)
	,constraint l_ticket_flight_airport_aircraft_l_flight_airport_aircraft_fk
		foreign key (lhk_flight_airport_aircraft)
		references l_flight_airport_aircraft (lhk_flight_airport_aircraft)
);

-- COMMAND ----------



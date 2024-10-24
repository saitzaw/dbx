-- Databricks notebook source
-- MAGIC %run ../../macros/uc_catalog_config

-- COMMAND ----------

create table s_ticket_flight_airport_aircraft (
	lhk_ticket_flight_airport_aircraft char(32)       not null
	,load_date_ts                      timestamp      not null
	,fare_conditions                   varchar(10)    not null
	,amount                            numeric(10, 2) not null
	,constraint s_ticket_flight_airport_aircraft_pk
		primary key (lhk_ticket_flight_airport_aircraft, load_date_ts)
	,constraint s_ticket_flight_airport_aircraft_l_fk
		foreign key (lhk_ticket_flight_airport_aircraft)
		references l_ticket_flight_airport_aircraft (lhk_ticket_flight_airport_aircraft)
);

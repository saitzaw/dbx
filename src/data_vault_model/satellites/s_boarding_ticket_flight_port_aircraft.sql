-- Databricks notebook source
-- MAGIC %run ../../macros/uc_catalog_config

-- COMMAND ----------

create table s_boarding_ticket_flight_port_aircraft (
	lhk_ticket_flight_airport_aircraft char(32)    not null
	,load_date_ts                      timestamp   not null
	,boarding_no                       integer     not null
	,seat_no                           varchar(4)  not null
	,constraint s_boarding_ticket_flight_port_aircraft_pk
		primary key (lhk_ticket_flight_airport_aircraft, load_date_ts)
	,constraint s_boarding_ticket_flight_port_aircraft_l_fk
		foreign key (lhk_ticket_flight_airport_aircraft)
		references l_ticket_flight_airport_aircraft (lhk_ticket_flight_airport_aircraft)
);

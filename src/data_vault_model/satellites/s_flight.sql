-- Databricks notebook source
-- MAGIC %run ../../macros/uc_catalog_config

-- COMMAND ----------

create table s_flight (
	 lhk_flight_airport_aircraft            char(32)     not null
	,load_date_ts         timestamp    not null
	,flight_no             char(6)     not null
	,scheduled_departure  timestamp  not null
	,scheduled_arrival    timestamp  not null
	,actual_departure     timestamp
	,actual_arrival       timestamp
	,status               varchar(20)
	,constraint s_flight_pk 
		primary key (lhk_flight_airport_aircraft, load_date_ts)
	,constraint s_flight_l_flight_airport_aircraft_fk
		foreign key (lhk_flight_airport_aircraft)
		references l_flight_airport_aircraft(lhk_flight_airport_aircraft)
);


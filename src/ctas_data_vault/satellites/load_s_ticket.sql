-- Databricks notebook source
  -- Ticket-Satellite
insert into dbx_arch.data_vault2.s_ticket (
	 hk_ticket
	,load_date_ts
	,passenger_id
	,passenger_name
	,passenger_phone
	,passenger_email
) select
 	md5(upper(trim(ticket_no)))
 	,current_timestamp
 	,passenger_id
	,passenger_name
 	,get_json_object(contact_data, '$.phone')
	,get_json_object(contact_data, '$.email')
 from dbx_arch.landing_zone.tickets;

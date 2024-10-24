-- Databricks notebook source
insert into dbx_arch.data_vault2.l_booking_ticket (
	lhk_booking_ticket
	,hk_booking
	,hk_ticket
	,load_date_ts
) select
	md5 (concat(
		upper(trim(ticket_no))
		, '|'
		, upper(trim(book_ref))
	))
	,md5(upper(trim(book_ref)))
	,md5(upper(trim(ticket_no)))
	,current_timestamp()
 from dbx_arch.landing_zone.tickets; 

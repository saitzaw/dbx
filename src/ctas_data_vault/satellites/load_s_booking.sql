-- Databricks notebook source
insert into dbx_arch.data_vault2.s_booking (
	hk_booking
	,load_date_ts
	,book_date
	,total_amount
) select 
	md5(upper(trim(book_ref)))
	,current_timestamp()
	,book_date
	,total_amount 
 from dbx_arch.landing_zone.bookings;

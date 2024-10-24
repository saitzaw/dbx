-- Databricks notebook source
insert into dbx_arch.data_vault2.h_booking (
	hk_booking
	,book_ref
	,load_date_ts
) select 
	md5(upper(trim(book_ref)))
	,book_ref 
	,current_timestamp()
 from dbx_arch.landing_zone.bookings;

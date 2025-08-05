/*
===================================================================

stored procedure load bronze layer(source-> bronze)
===================================================================
script purpose
this stored procedure loads data into 'bronze' schema  from external csv files.
it performs the following actions
-truncate the bronze tables before loading data
-uses the bulk insert command to load data from csv files to bronze tables
parameters:
  none.
this stored procedure does not accept any arguments or returns any values.

use example
  exec bronze.load_bronze;
  ======================================================================
*/
create or alter procedure bronze.load_bronze as
begin
declare @start_time datetime, @end_time datetime;
set @start_time = getdate();
	begin try

		print'==============================================='
		print'           Loading bronze layer                '
		print'==============================================='


		print'-----------------------------------------------'
		print'           Loading CRM Tables                '
		print'-----------------------------------------------'


		truncate table bronze.crm_cust_info;
		bulk insert [bronze].[crm_cust_info]
		from 'D:\DataWarehouse\projectDWH\source_crm\cust_info.csv'
		with(
			firstrow =2,
			fieldterminator =',',
			Tablock
		);
		print'           Truncating and inserting at same time                '


		truncate table [bronze].[crm_prd_info];
		bulk insert [bronze].[crm_prd_info]
		from 'D:\DataWarehouse\projectDWH\source_crm\prd_info.csv'
		with(
			firstrow =2,
			fieldterminator =',',
			Tablock
		);
		print'           Truncating and inserting at same time                '


		truncate table [bronze].[crm_sales_details];
		bulk insert [bronze].[crm_sales_details]
		from 'D:\DataWarehouse\projectDWH\source_crm\sales_details.csv'
		with(
			firstrow =2,
			fieldterminator =',',
			Tablock
		);
		print'           Truncating and inserting at same time                '


		print'-----------------------------------------------'
		print'           Loading ERP Tables                '
		print'-----------------------------------------------'


		truncate table [bronze].[erp_cus_az12];
		bulk insert [bronze].[erp_cus_az12]
		from 'D:\DataWarehouse\projectDWH\source_erp\CUST_AZ12.csv'
		with(
			firstrow =2,
			fieldterminator =',',
			Tablock
		);

		print'           Truncating and inserting at same time                '


		truncate table [bronze].[erp_loc_a101];
		bulk insert [bronze].[erp_loc_a101]
		from 'D:\DataWarehouse\projectDWH\source_erp\LOC_A101.csv'
		with(
			firstrow =2,
			fieldterminator =',',
			Tablock
		);
		print'           Truncating and inserting at same time                '


		truncate table [bronze].[erp_px_cat_g1v2];
		bulk insert [bronze].[erp_px_cat_g1v2]
		from 'D:\DataWarehouse\projectDWH\source_erp\PX_CAT_G1V2.csv'
		with(
			firstrow =2,
			fieldterminator =',',
			Tablock
		);
		print'           Truncating and inserting at same time                '
	end try
	begin catch
	print'--------------------------------'
	print'          Errores accured       '
	print'--------------------------------'
	print' error '+ error_message();
	print' error '+ error_number();
	print' error '+cast( error_line() as varchar);
	print' error '+cast( error_state()as varchar);
	end catch;
set @end_time = getdate();
	print 'The start tme ' + cast(@start_time as varchar)
	print 'The end tme ' +cast( @end_time as varchar)
	print'Loading duration '+ cast(datediff(second,@start_time,@end_time)as varchar)+' seconds'
--select * from bronze.crm_cust_info

--select count(*) from bronze.crm_cust_info
end;



EXEC bronze.load_bronze

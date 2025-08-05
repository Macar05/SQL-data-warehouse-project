/*
===================================================================

stored procedure load silver layer(bronze->silver)
===================================================================
script purpose
this stored procedure loads data into 'silver' schema  from bronze layer.
it performs the following actions
-truncate the silver tables before loading data
-uses the inser  command to load data from bronze to silver tables
parameters:
  none.
this stored procedure does not accept any arguments or returns any values.

use example
 exec silver.load_pro;
  ======================================================================
*/




create or alter procedure silver.Load_pro as
begin
  begin try
	declare @load_start_time datetime2, @load_end_time datetime2
	 set @load_start_time = getdate();

		 print'=========================================================='
	print'                   Customer info table                    '
	print'==========================================================='
	print '>>Truncating the silver.crm_cust_info'
	truncate table silver.crm_cust_info;
	print '>>Inserting or loading data to the table silver.crm_cust_info'
	insert into silver.crm_cust_info (

		cst_id ,
		cst_key ,
		cst_firstname ,
		cst_lastname ,
		cst_marterial_status ,
		cst_gndr ,
		cst_create_date

	)
	select 
		cst_id,
		cst_key,
		trim(cst_firstname) as cst_firstname,
		trim(cst_lastname) as cst_lastname,
		case
		when upper(cst_marterial_status) ='S' then 'Single'
		when upper(cst_marterial_status) ='M' then 'Married'
		else 'a/n'
		end cst_marterial_status,
		case
		when upper(cst_gndr) ='M' then 'Male'
		when upper(cst_gndr) ='F' then 'Female'
		else 'a/n'
		end cst_gndr,
		cst_create_date
	from

		(
			select 
			*,
			ROW_NUMBER() over(partition by cst_id order by cst_create_date desc ) Flag_last
			from bronze.crm_cust_info where cst_id is not null
		)t
	where Flag_last =1

	print'=========================================================='
	print'                         product info table               '
	print'==========================================================='
	print '>>Dropping the table'
	 drop table silver.crm_prd_info
	 print'>>Creating table and adding after derived columns'
	create table silver.crm_prd_info(
	prd_id int,
	cat_d nvarchar(50),
	prd_key nvarchar(50),
	prd_nm nvarchar(50),
	prd_cost int,
	prd_line nvarchar(50),
	prd_start_dt date,
	prd_end_dt date,
	dwh_create_date datetime2 default getdate()

	);

	print'>>loading data from bronze to silver layer after cleaned'

	insert into silver.crm_prd_info (

	prd_id ,
	cat_d ,
	prd_key ,
	prd_nm ,
	prd_cost ,
	prd_line ,
	prd_start_dt ,
	prd_end_dt 

	)

	select 
	prd_id,
	replace(SUBSTRING(prd_key,1,5),'-','_') as cat_d, --extract category id
	SUBSTRING(prd_key,7,len(prd_key)) as prd_key, --extract product key
	prd_nm,
	isnull(prd_cost,0)prd_cost,
	case UPPER(trim(prd_line))
		when  'M' then 'Mountain'
		when  'R' then 'Road'
		when  'S' then 'Other Sales'
		when  'T' then 'Touring'
		else 'a/n'
	end prd_line, -- mapping product line codes to descriptive values
	cast(prd_start_dt as varchar) prd_start_dt,
	cast(lead(prd_start_dt) over (partition by prd_key  order by prd_start_dt)-1
	as date) as prd_end_dt_test --calculate end date as one day before the next start date
	from bronze.crm_prd_info


	print'=========================================================='
	print'                   sales details table                    '
	print'==========================================================='
	print '>>Truncating the silver.crm_sales_details'
	truncate table silver.crm_sales_details;
	print '>>Inserting or loading data to the table ssilver.crm_sales_details'
	insert into silver.crm_sales_details
	(
	sls_ord_num,
	sls_prd_key, sls_cus_id,
	sls_order_dt, sls_ship_dy, sls_due_dt,
	sls_sales, sls_quantity, sls_price

	)


	select 
	sls_ord_num,
	sls_prd_key,
	sls_cus_id,
	case 
	when  sls_order_dt <=0 or LEN(sls_order_dt) !=8 then null
	else cast(cast(sls_order_dt as varchar) as date)
	end sls_order_dt,  
	cast(cast(sls_ship_dy as varchar) as date) as sls_ship_dy,
	cast(cast(sls_due_dt as varchar) as date) as sls_due_dt,
	---changing integer dates to date after cleasing the dat
	case 
	when sls_sales<=0 or sls_sales is null or sls_sales != sls_quantity * abs(sls_price)
	then sls_quantity* abs(sls_price)
	else sls_sales
	end sls_sales, --- Recalculating sales if the original value is missing or incorrect
	sls_quantity,
	case 
	when sls_price<=0 or sls_sales is null
	then sls_sales/ nullif(sls_quantity,0)
	else sls_price
	end sls_price -- derive priceif the original value is in valid
	from bronze.crm_sales_details



	print'=========================================================='
	print'                   customers extra info table              '
	print'==========================================================='
	print '>>Truncating the silver.erp_cus_az12'
	truncate table silver.erp_cus_az12;
	print '>>Inserting or loading data to the table silver.erp_cus_az12'
	insert into silver.erp_cus_az12
	(
		cid,
		bdate, 
		gen

	)
	select 
	case 
	when cid like 'NAS%' then  substring(cid,4,len(cid))
	else cid
	end cid, --- removing NAS prefix if exists
	case
	when bdate >= getdate() then null
	else bdate
	end bdate, ---set futrue birthdays to null
	case
	when upper(trim(gen)) ='F' or upper(trim(gen))='Female' then 'Female'
	when upper(trim(gen)) ='M' or upper(trim(gen))='Male' then 'Male'
	else  'n/a'
	end gen --normalize gender values and handle unknown cases
	from bronze.erp_cus_az12


	print'=========================================================='
	print'          customers Location by country table              '
	print'==========================================================='
	print '>>Truncating the silver.erp_loc_a101'
	truncate table silver.erp_loc_a101;
	print '>>Inserting or loading data to the table silver.erp_loc_a101'
	insert into silver.erp_loc_a101(
	cid,
	cntry
	)

	select  distinct
	replace(cid,'-','') as cid,
	case
	when trim(cntry)='DE' then 'Germany'
	when trim(cntry)='USA' or cntry='US' then 'United States'
	when trim(cntry)='' or  cntry is null then 'n/a'
	else trim(cntry)
	end cntry ---normalizing and handle missing or blank code
	from bronze.erp_loc_a101

	print'=========================================================='
	print'          product category table              '
	print'==========================================================='
	print '>>Truncating the ssilver.erp_px_cat_g1v2'
	truncate table silver.erp_px_cat_g1v2;
	print '>>Inserting or loading data to the table silver.erp_px_cat_g1v2'
	insert into silver.erp_px_cat_g1v2
	(
	id,
	cat,
	subcat,
	maintenance
	)
	select id,
	cat,
	subcat,
	maintenance
	from bronze.erp_px_cat_g1v2
	 set @load_end_time = GETDATE(); 
	 print'>>The batch duration is: '+ cast(datediff(second,@load_start_time,@load_end_time) as nvarchar)
 end try
 begin catch
	  print'             Error Messages             '
	  print'the error: '+ error_message();
	  print'the error: '+ error_line();
	  print'the erros r: '+ error_number()
	  print'the erros r: '+ error_state()
 end catch
end


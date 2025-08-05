/*
The following codes will check the data quality of each table in silver layer.
unwanted spaces, duplicates, abbriviations, data consistency, acurracy and so on
*/

------------------------customer info---------------------
---step1 check the unwanted spaces--------------
select 
cst_firstname
from silver.crm_cust_info
where cst_firstname !=trim(cst_firstname)

---step2 check the nulss and duplicates--------------
select 
cst_id,
count(*)
from silver.crm_cust_info
group by cst_id
having count(*)>1 or cst_id is null

---step3 check the data standardization and consistancy--------------

select distinct cst_gndr from silver.crm_cust_info

select distinct cst_gndr from bronze.crm_cust_info

select distinct cst_marterial_status from silver.crm_cust_info

------------------step 4 read full table
select * from silver.crm_cust_info 

-------------------------------product info--------------------------
---step1 check the unwanted spaces--------------
select 
prd_nm
from silver.crm_prd_info
where prd_nm !=trim(prd_nm)

---step2 check the nulss and duplicates--------------
select 
prd_id,
count(*)
from silver.crm_prd_info
group by prd_id
having count(*)>1 or prd_id is null

---step3 check the data standardization and consistancy--------------

select distinct prd_line from silver.crm_prd_info

---step4 check invalid data orders--------------
select distinct prd_start_dt from silver.crm_prd_info
where prd_start_dt>prd_end_dt

------------------step5 read full table
select * from silver.crm_prd_info



-------------------------------sales details--------------------------
-------------------quallity check
select * from silver.crm_sales_details
where sls_order_dt> sls_ship_dy or sls_order_dt>sls_due_dt

select distinct
sls_sales,
sls_quantity,
sls_price
from Silver.crm_sales_details
where sls_sales <=0 or sls_sales is null
or sls_sales < sls_quantity*sls_price
or sls_sales > sls_quantity*sls_price

select * from silver.crm_sales_details



-------------------------------customer extra info--------------------------
----------------quality checks

select 
distinct (gen)
from silver.erp_cus_az12

select 
bdate
from silver.erp_cus_az12
where bdate <'1900-01-01' or  bdate>getdate()

select 
cid,
bdate,
gen
from silver.erp_cus_az12
where cid = 'AW00011000'

select 
cid,
bdate,
gen,
dwh_create_date
from silver.erp_cus_az12


select * from silver.erp_cus_az12
-------------------------------customer location by country--------------------------
-----------------------quality check
select 
distinct
cntry
from silver.erp_loc_a101


select * from silver.erp_loc_a101
-------------------------------product category--------------------------

------------------checking the quality


select id,
cat,
subcat,
maintenance
from silver.erp_px_cat_g1v2
where id!=trim(id) or cat!=trim(cat)  or subcat!=trim(subcat) or maintenance!=trim(maintenance)

select distinct
maintenance
from silver.erp_px_cat_g1v2

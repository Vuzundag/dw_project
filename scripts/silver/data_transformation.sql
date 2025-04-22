-- transform and load customer information for silver layer
select '>>>Truncating the table: silver.crm_cust_info<<<';
truncate table silver.crm_cust_info;
select '>>>Inserting data into: silver.crm_cust_info<<<';
insert into silver.crm_cust_info(
	cst_id,
	cst_key,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gender,
    cst_create_date
)

select
	cst_id,
    cst_key,
    trim(cst_firstname) as cst_firstname,
    trim(cst_lastname) as cst_lastname,
    case when upper(trim(cst_marital_status)) = 'S' then 'Single'
		when upper(trim(cst_marital_status)) = 'M' then 'Married'
        else 'Unknown' 
	end as cst_marital_status,
    case when upper(trim(cst_gender)) = 'F' then 'Female'
		when upper(trim(cst_gender)) = 'M' then 'Male'
        else 'Unknown' 
	end as cst_gender,
    cst_create_date
from(
	select
		*,
		row_number() over(partition by cst_id order by cst_create_date desc) as flag_last
	from bronze.crm_cust_info) sub
where flag_last = 1;



-- transform and load product information for silver layer
select '>>>Truncating the table: silver.crm_prd_info';
truncate table silver.crm_prd_info;
select '>>>Inserting data into: silver.crm_prd_info';
insert into silver.crm_prd_info(
	prd_id,
    prd_key,
    cat_id,
    prd_name,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
)

select
	prd_id,
    prd_key,
    substr(prd_key, 1, 5) as cat_id,
    prd_name,
    ifnull(prd_cost, 0) as prd_cost,
    case
		when trim(prd_line) = 'M' then 'Mountain'
        when trim(prd_line) =  'R' then 'Road'
        when trim(prd_line) = 'S' then 'Other Sales'
        when trim(prd_line) = 'T' then 'Touring'
        else 'Unknown' 
	end as prd_line, -- map the abbreviation to descriptive values
    cast(prd_start_dt as date) as prd_start_dt,
    cast(lead(prd_start_dt) over(partition by prd_key order by prd_start_dt)
			as date) as prd_end_dt
from bronze.crm_prd_info;


/* ------------------------------------------------------------------------------------------ */
-- transform and load crm sales details 
select '>>>Truncating the table: silver.crm_sales_details';
truncate table silver.crm_sales_details;
select '>>>Inserting data into: silver.crm_sales_details';
insert into silver.crm_sales_details (
	sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_ord_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
)

select
	sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    case 
		when sls_ord_dt <= 0 or length(sls_ord_dt) != 8 then null
		else cast(cast(sls_ord_dt as char) as date) 
	end as sls_ord_dt, -- transform date columns for any error
    case 
		when sls_ship_dt <= 0 or length(sls_ship_dt) != 8 then null
		else cast(cast(sls_ship_dt as char) as date) 
	end as sls_ship_dt,
	case 
		when sls_due_dt <= 0 or length(sls_due_dt) != 8 then null
		else cast(cast(sls_due_dt as char) as date) 
	end as sls_due_dt,
    case 
		when sls_sales <= 0 or sls_sales is null or sls_sales != sls_quantity * abs(sls_price) then sls_quantity * abs(sls_price)
		else sls_sales 
	end as sls_sales,
    case 
		when sls_quantity < 0 then abs(sls_quantity)
		when sls_quantity = 0 then sls_quantity is null
        else sls_quantity 
	end as sls_quantity,
    case 
		when sls_price < 0 then abs(sls_price)
		when sls_price = 0 or sls_price is null then abs(sls_sales) / sls_quantity
        else sls_price 
	end as sls_price -- transforming negative and null values in price and quantity columns
from bronze.crm_sales_details;
    
    
/* ------------------------------------------------------------------------------------------ */
-- transform and load erp customer data
select '>>>Truncating the table: silver.erp_cust_az_12';
truncate table silver.erp_cust_az_12;
select '>>>Inserting data into: silver.erp_cust_az_12';
insert into silver.erp_cust_az_12(
	cid,
    bdate,
    gen
)
select
    case when cid like 'NAS%' then substring(cid, 4, length(cid)) 
		else cid end as cid,
    case when bdate >= now() then null else bdate end as bdate,
    case when trim(gen) in ('M', 'Male') then 'Male'
		 when trim(gen) in ('F', 'Female') then 'Female'
         else 'Unknown'
	end as gen
from bronze.erp_cust_az_12;


/* ------------------------------------------------------------------------------------------ */
-- transform and load erp customer data
select '>>>Truncating the table: silver.erp_loc_a101';
truncate table silver.erp_loc_a101;
select '>>>Inserting data into: silver.erp_loc_a101';
insert into silver.erp_loc_a101 (
	cid,
    country
    )
    
select
	replace(cid, '-', '') as cid,
    case when trim(country) in ('US', 'USA') then 'United States'
		 when trim(country) = 'DE' then 'Germany'
         when trim(country) = '' or country is null then 'Unknown' 
         else trim(country)
    end as country
from bronze.erp_loc_a101;



/* ------------------------------------------------------------------------------------------ */
-- transform and load erp customer data
select '>>>Truncating the table: silver.erp_px_cat_g1v2';
truncate table silver.erp_px_cat_g1v2;
select '>>>Inserting data into: silver.erp_px_cat_g1v2';
insert into silver.erp_px_cat_g1v2(
	id,
    cat,
    subcat,
    maintenance
)

select
	id,
    cat,
    subcat,
    maintenance
from bronze.erp_px_cat_g1v2;

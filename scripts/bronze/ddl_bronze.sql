/*
_____________________________________________________________________________________________
DDL Script: Create Bronze Tables
_____________________________________________________________________________________________
Goal:
	This script creates tables and load data into them in the bronze layer of the data warehouse.
    It first drop the tables if they exist then create and load them..

______________________________________________________________________________________________
*/

drop table if exists bronze.crm_cust_info;
create table bronze.crm_cust_info (
	cst_id int,
    cst_key varchar(50),
    cst_firstname varchar(50),
    cst_lastname varchar(50),
    cst_marital_status varchar(50),
    cst_gender varchar(50),
    cst_create_date date
);


drop table if exists bronze.crm_prd_info;
create table bronze.crm_prd_info (
	prd_id int,
    prd_key varchar(50),
    prd_name varchar(100),
    prd_cost decimal(10,2),
    prd_line varchar(50),
    prd_start_dt datetime,
    prd_end_dt datetime
);


drop table if exists bronze.crm_sales_details;
create table bronze.crm_sales_details (
	sls_ord_num varchar(50),
    sls_prd_key varchar(50),
    sls_cust_id int,
    sls_ord_dt int,
    sls_ship_dt int,
    sls_due_dt int,
    sls_sales decimal(10,2),
    sls_quantity int,
    sls_price decimal(10,2)
);


drop table if exists bronze.erp_cust_az_12;
create table bronze.erp_cust_az_12(
	cid varchar(50),
    bdate date,
    gen varchar(50)
);


drop table if exists bronze.erp_loc_a101;
create table bronze.erp_loc_a101 (
	cid varchar(50),
    country varchar(50)
);


drop table if exists bronze.erp_px_cat_g1v2;
create table bronze.erp_px_cat_g1v2 (
	id varchar(50),
    cat varchar(100),
    subcat varchar(100),
    maintenance varchar(50)
);


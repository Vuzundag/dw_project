/*
_________________________________________________________________________________________
Stored Procedure: Load Bronze Layer From Source
_________________________________________________________________________________________
Goal:
	This script loads data into the bronze schema from ERP and CRM csv files. 
	It performs:
	- Truncates the tables before loading data
    - Loads data directly from csv files into the tables.

Parameters:
	It take no parameter
    
Usage:
	CALL bronze.load_bronze()

*/


DELIMITER $$

CREATE PROCEDURE bronze.load_bronze()
BEGIN
	select '================================================================' as message;
    select 'Loading Bronze Layer' as message;
    select '================================================================' as message;
    select '================================================================' as message;
    select 'Loading CRM Tables' as message;
    select '================================================================' as message;
    
    set @start_time = now();
    select '>> Truncating and Loading Table: bronze.crm_cust_info' as message;
    -- Truncate and load crm_cust_info
    TRUNCATE TABLE bronze.crm_cust_info;
    LOAD DATA LOCAL INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\cust_info.csv'
    INTO TABLE bronze.crm_cust_info
    FIELDS TERMINATED BY ','
    IGNORE 1 ROWS;
    set @end_time = now();
    select 'Load Duration: ' + datediff(second, @start_time, @end_time) + ' seconds' as message;
    

    set @start_time = now();
    select '>> Truncating and Loading Table: bronze.crm_prod_info' as message;
    -- Truncate and load crm_prd_info
    TRUNCATE TABLE bronze.crm_prd_info;
    LOAD DATA LOCAL INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\prd_info.csv'
    INTO TABLE bronze.crm_prd_info
    FIELDS TERMINATED BY ','
    IGNORE 1 ROWS;
    set @end_time = now();
    select 'Load Duration: ' + datediff(second, @start_time, @end_time) + ' seconds' as message;

    set @start_time = now();
    select '>> Truncating and Loading Table: bronze.crm_sales_details' as message;
    -- Truncate and load crm_sales_details
    TRUNCATE TABLE bronze.crm_sales_details;
    LOAD DATA LOCAL INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\sales_details.csv'
    INTO TABLE bronze.crm_sales_details
    FIELDS TERMINATED BY ','
    IGNORE 1 ROWS;
    set @end_time = now();
    select 'Load Duration: ' + datediff(second, @start_time, @end_time) + ' seconds' as message;

    
    select '================================================================' as message;
    select 'Loading ERP Tables' as message;
    select '================================================================' as message;
    
    set @start_time = now();
    select '>> Truncating and Loading Table: bronze.erp_cust_az_12' as message;
    -- Truncate and load erp_cust_az_12
    TRUNCATE TABLE bronze.erp_cust_az_12;
    LOAD DATA LOCAL INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\cust_az_12.csv'
    INTO TABLE bronze.erp_cust_az_12
    FIELDS TERMINATED BY ','
    IGNORE 1 ROWS;
    set @end_time = now();
    select 'Load Duration: ' + datediff(second, @start_time, @end_time) + ' seconds' as message;

    set @start_time = now();
    select '>> Truncating and Loading Table: bronze.erp_loc_a101' as message;
    -- Truncate and load erp_loc_a101
    TRUNCATE TABLE bronze.erp_loc_a101;
    LOAD DATA LOCAL INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\loc_a101.csv'
    INTO TABLE bronze.erp_loc_a101
    FIELDS TERMINATED BY ','
    IGNORE 1 ROWS;
    set @end_time = now();
    select 'Load Duration: ' + datediff(second, @start_time, @end_time) + ' seconds' as message;

    set @start_time = now();
    select '>> Truncating and Loading Table: bronze.erp_px_cat_g1v2' as message;
    -- Truncate and load erp_px_cat_g1v2
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;
    LOAD DATA LOCAL INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\px_cat_g1v2.csv'
    INTO TABLE bronze.erp_px_cat_g1v2
    FIELDS TERMINATED BY ','
    IGNORE 1 ROWS;
    set @end_time = now();
    select 'Load Duration: ' + datediff(second, @start_time, @end_time) + ' seconds' as message;

END$$

DELIMITER ;
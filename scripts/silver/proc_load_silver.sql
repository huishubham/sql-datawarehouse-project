/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/

-- Data Load

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;

	BEGIN TRY
    	SET @batch_start_time = GETDATE();
    	PRINT '====================================================================';
    	PRINT '>> Loading Silver Layer';
    	PRINT '====================================================================';
    
    	PRINT '--------------------------------------------------------------------';
    	PRINT '>> Loading CRM Tables';
    	PRINT '--------------------------------------------------------------------';
    
    	-- Loading Cleansed Data into 'silver.crm_cust_info'
    	SET @start_time = GETDATE();
    	PRINT '--------------------------------------------------------------------';
    	PRINT '>> Truncating Table: silver.crm_cust_info'
    	PRINT '--------------------------------------------------------------------';
    	TRUNCATE TABLE silver.crm_cust_info;
    	PRINT '--------------------------------------------------------------------';
    	PRINT '>> Loading Table: silver.crm_cust_info'
    	PRINT '--------------------------------------------------------------------';

    	INSERT INTO silver.crm_cust_info(
    	cst_id,
    	cst_key,
    	cst_firstname,
    	cst_lastname,
    	cst_marital_status,
    	cst_gender,
    	cst_create_date
    	)
    	SELECT 
    	cst_id,
    	cst_key,
    	TRIM(cst_firstname) AS cst_firstname,
    	TRIM(cst_lastname) AS cst_lastname,
    	CASE WHEN cst_marital_status = 'S' THEN 'Single'
    			WHEN cst_marital_status = 'M' THEN 'Married'
    			ELSE 'Unknown'
    	END AS cst_marital_status,
    	CASE WHEN cst_gender = 'F' THEN 'Female'
    			WHEN cst_gender = 'M' THEN 'Male'
    			ELSE 'Unknown'
    	END AS cst_gender,
    	cst_create_date
    	FROM(
    	SELECT *,
    	ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
    	FROM bronze.crm_cust_info 
    	WHERE cst_id IS NOT NULL)t 
    	WHERE flag_last = 1;

    	SET @end_time = GETDATE();
    	PRINT ' >> Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds'
    	PRINT '-------------'
    
    	-- Loading Cleansed Data into 'silver.crm_prd_info'
    	SET @start_time = GETDATE();
    	PRINT '--------------------------------------------------------------------';
    	PRINT '>> Truncating Table: silver.crm_prd_info'
    	PRINT '--------------------------------------------------------------------';
    	TRUNCATE TABLE silver.crm_prd_info;
    	PRINT '--------------------------------------------------------------------';
    	PRINT '>> Loading Table: silver.crm_prd_info'
    	PRINT '--------------------------------------------------------------------';
    	INSERT INTO silver.crm_prd_info(
    	prd_id,
    	cat_id,
    	prd_key,
    	prd_nm,
    	prd_cost,
    	prd_line,
    	prd_start_dt,
    	prd_end_dt
    	)
    	SELECT 
    	prd_id,
    	REPLACE(SUBSTRING(prd_key,1,5), '-','_')AS cat_id,
    	SUBSTRING(prd_key,7,LEN(prd_key))AS prd_key,
    	prd_nm,
    	ISNULL(prd_cost,0)AS prd_cost,
    	CASE 
    		WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
    		WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
    		WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
    		WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
    		ELSE 'Unknown'
    	END AS prd_line, 
    	prd_start_dt,
    	DATEADD(DAY, -1, LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)) AS prd_end_dt
    	FROM bronze.crm_prd_info;

    	SET @end_time = GETDATE();
    	PRINT ' >> Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds'
    	PRINT '-------------'
    
    
    	-- Loading Cleansed Data into 'silver.crm_sales_details'
    	SET @start_time = GETDATE();
    	PRINT '--------------------------------------------------------------------';
    	PRINT '>> Truncating Table: silver.crm_sales_details'
    	PRINT '--------------------------------------------------------------------';
    	TRUNCATE TABLE silver.crm_sales_details;
    	PRINT '--------------------------------------------------------------------';
    	PRINT '>> Loading Table: silver.crm_sales_details'
    	PRINT '--------------------------------------------------------------------';
    
    	INSERT INTO silver.crm_sales_details(
    	sls_ord_num,	
    	sls_prd_key,	
    	sls_cust_id,	
    	sls_order_dt,	
    	sls_ship_dt,	
    	sls_due_dt,	
    	sls_sales,	
    	sls_quantity,	
    	sls_price	
    	)
    	SELECT
    	sls_ord_num,
    	sls_prd_key,
    	sls_cust_id,
    	sls_order_dt,
    	sls_ship_dt,
    	sls_due_dt,
    	CASE 
    			WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * sls_price
    			THEN sls_quantity * ABS(sls_price)
    			ELSE sls_sales
    	END AS sls_sales,
    	sls_quantity,
    	CASE 
    		WHEN sls_price IS NULL OR sls_price <=0 THEN sls_sales/NULLIF(sls_quantity,0)
    		ELSE sls_price
    	END AS sls_price
    	FROM bronze.crm_sales_details;

    	SET @end_time = GETDATE();
    	PRINT ' >> Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds'
    	PRINT '-------------'

        
    	PRINT '--------------------------------------------------------------------';
    	PRINT '>> Loading ERP Tables';
    	PRINT '--------------------------------------------------------------------';
    
    	-- Loading Cleansed Data into 'silver.erp_cust_AZ12'
    	SET @start_time = GETDATE();
    	PRINT '--------------------------------------------------------------------';
    	PRINT '>> Truncating Table: silver.erp_cust_AZ12'
    	PRINT '--------------------------------------------------------------------';
    	TRUNCATE TABLE silver.erp_cust_AZ12;
    	PRINT '--------------------------------------------------------------------';
    	PRINT '>> Loading Table: silver.erp_cust_AZ12'
    	PRINT '--------------------------------------------------------------------';
    	INSERT INTO silver.erp_cust_AZ12(
    	cid,
    	bdate,
    	gen
    	)
    	SELECT 
    	CASE WHEN UPPER(TRIM(cid)) LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
    			ELSE cid
    	END AS cid,
    	CASE WHEN bdate > GETDATE() THEN NULL
    			ELSE bdate
    	END AS bdate,
    	CASE WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
    			WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
    			ELSE 'Unknown'
    	END
    	FROM bronze.erp_cust_AZ12 ;

    	SET @end_time = GETDATE();
    	PRINT ' >> Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds'
    	PRINT '-------------'
    
    	-- Loading Cleansed Data into 'silver.erp_loc_A101'
    	SET @start_time = GETDATE()
    	PRINT '--------------------------------------------------------------------';
    	PRINT '>> Truncating Table: silver.erp_loc_A101'
    	PRINT '--------------------------------------------------------------------';
    	TRUNCATE TABLE silver.erp_loc_A101;
    	PRINT '--------------------------------------------------------------------';
    	PRINT '>> Loading Table: silver.erp_loc_A101'
    	PRINT '--------------------------------------------------------------------';
    	INSERT INTO silver.erp_loc_A101(
    	cid,
    	cntry
    	)
    	SELECT 
    	REPLACE(TRIM(cid),'-','') AS cid,
    	CASE WHEN TRIM(UPPER(cntry)) = '' OR cntry is NULL THEN 'Unknown'
    			WHEN TRIM(UPPER(cntry)) = 'DE' THEN 'Germany'
    			WHEN TRIM(UPPER(cntry)) IN ('US','USA','United States of America') THEN 'United States'
    			ELSE TRIM(cntry)
    	END AS cntry
    	FROM bronze.erp_loc_A101;

    	SET @end_time = GETDATE();
    	PRINT ' >> Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds'
    	PRINT '-------------'
    
    	-- Loading Cleansed Data into 'silver.erp_px_cat_g1v2'
    	SET @start_time = GETDATE();
    	PRINT '--------------------------------------------------------------------';
    	PRINT '>> Truncating Table: silver.erp_px_cat_g1v2'
    	PRINT '--------------------------------------------------------------------';
    	TRUNCATE TABLE silver.erp_px_cat_g1v2;
    	PRINT '--------------------------------------------------------------------';
    	PRINT '>> Loading Table: silver.erp_px_cat_g1v2'
    	PRINT '--------------------------------------------------------------------';
    	INSERT INTO silver.erp_px_cat_g1v2(
    	id,
    	cat,
    	subcat,
    	maintenance
    	)
    	SELECT
    	id,
    	cat,
    	subcat,
    	maintenance
    	FROM bronze.erp_px_cat_g1v2;
    	SET @end_time = GETDATE();
    	PRINT ' >> Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds'
    	PRINT '-------------'
        
    	SET @batch_end_time = GETDATE();
    	PRINT '====================================================================';
    	PRINT '--------------------Bronze Layer Loaded Successfully----------------';
    	PRINT ' >> Silver Layer Load Duration: ' + CAST(DATEDIFF(second,@batch_start_time,@batch_end_time) AS NVARCHAR(20)) + 'seconds';
    	PRINT '====================================================================';

	END TRY

	BEGIN CATCH
		PRINT '====================================================================';
		PRINT 'Error Occured During >> Loading Bronze Layer';
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '===================================================================='; 
	END CATCH
END

EXEC silver.load_silver;


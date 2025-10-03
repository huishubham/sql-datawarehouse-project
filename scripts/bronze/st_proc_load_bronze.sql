/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/



CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	
	
	BEGIN TRY
	SET @batch_start_time = GETDATE();
	PRINT '====================================================================';
	PRINT 'Loading Bronze Layer';
	PRINT '====================================================================';

	PRINT '--------------------------------------------------------------------';
	PRINT 'Loading CRM Tables';
	PRINT '--------------------------------------------------------------------';

	-- Table1
	SET @start_time = GETDATE();
	PRINT '--------------------------------------------------------------------';
	PRINT 'Truncating Table:bronze.crm_cust_info';
	PRINT '--------------------------------------------------------------------';
	TRUNCATE TABLE bronze.crm_cust_info;
	PRINT '--------------------------------------------------------------------';
	PRINT 'Loading Table:bronze.crm_cust_info';
	PRINT '--------------------------------------------------------------------';
	BULK INSERT bronze.crm_cust_info
	FROM 'D:\Data Analyst Bootcamp\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
	WITH(
		FIRSTROW=2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);

	SET @end_time = GETDATE();
	PRINT ' >> Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds'
	PRINT '-------------'
	
	-- Table2
	SET @start_time = GETDATE();
	PRINT '--------------------------------------------------------------------';
	PRINT 'Truncating Table:bronze.crm_prd_info';
	PRINT '--------------------------------------------------------------------';
	TRUNCATE TABLE bronze.crm_prd_info;
	PRINT '--------------------------------------------------------------------';
	PRINT 'Loading Table:bronze.crm_prd_info';
	PRINT '--------------------------------------------------------------------';
	BULK INSERT bronze.crm_prd_info
	FROM 'D:\Data Analyst Bootcamp\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
	WITH(
		FIRSTROW=2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);

	SET @end_time = GETDATE();
	PRINT ' >> Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds'
	PRINT '-------------'

	-- Table3
	SET @start_time = GETDATE();
	PRINT '--------------------------------------------------------------------';
	PRINT 'Truncating Table:bronze.crm_sales_details';
	PRINT '--------------------------------------------------------------------';
	TRUNCATE TABLE bronze.crm_sales_details;
	PRINT '--------------------------------------------------------------------';
	PRINT 'Loading Table:bronze.crm_sales_details';
	PRINT '--------------------------------------------------------------------';
	BULK INSERT bronze.crm_sales_details
	FROM 'D:\Data Analyst Bootcamp\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
	WITH(
		FIRSTROW=2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);

	SET @end_time = GETDATE();
	PRINT ' >> Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds'
	PRINT '-------------'

	PRINT '--------------------------------------------------------------------';
	PRINT 'Loading ERP Tables';
	PRINT '--------------------------------------------------------------------';

	-- Table 4
	SET @start_time = GETDATE();
	PRINT '--------------------------------------------------------------------';
	PRINT 'Truncating Table:bronze.erp_cust_AZ12';
	PRINT '--------------------------------------------------------------------';
	TRUNCATE TABLE bronze.erp_cust_AZ12;
	PRINT '--------------------------------------------------------------------';
	PRINT 'Loading Table:bronze.erp_cust_AZ12';
	PRINT '--------------------------------------------------------------------';
	BULK INSERT bronze.erp_cust_AZ12
	FROM 'D:\Data Analyst Bootcamp\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
	WITH(
		FIRSTROW=2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);

	SET @end_time = GETDATE();
	PRINT ' >> Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds'
	PRINT '-------------'

	-- Table 5
	SET @start_time = GETDATE();
	PRINT '--------------------------------------------------------------------';
	PRINT 'Truncating Table:bronze.erp_loc_A101';
	PRINT '--------------------------------------------------------------------';
	TRUNCATE TABLE bronze.erp_loc_A101;
	PRINT '--------------------------------------------------------------------';
	PRINT 'Loading Table:bronze.erp_loc_A101';
	PRINT '--------------------------------------------------------------------';
	BULK INSERT bronze.erp_loc_A101
	FROM 'D:\Data Analyst Bootcamp\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
	WITH(
		FIRSTROW=2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);

	SET @end_time = GETDATE();
	PRINT ' >> Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds'
	PRINT '-------------'

	-- Table 6
	SET @start_time = GETDATE();
	PRINT '--------------------------------------------------------------------';
	PRINT 'Truncating Table:bronze.erp_px_cat_g1v2';
	PRINT '--------------------------------------------------------------------';

	TRUNCATE TABLE bronze.erp_px_cat_g1v2;
	PRINT '--------------------------------------------------------------------';
	PRINT 'Loading Table:bronze.erp_px_cat_g1v2';
	PRINT '--------------------------------------------------------------------';
	BULK INSERT bronze.erp_px_cat_g1v2
	FROM 'D:\Data Analyst Bootcamp\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
	WITH(
		FIRSTROW=2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);

	SET @end_time = GETDATE();
	PRINT ' >> Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds'
	PRINT '-------------'

	SET @batch_end_time = GETDATE();
	PRINT '====================================================================';
	PRINT '--------------------Bronze Layer Loaded Successfully----------------';
	PRINT ' >> Bronze Layer Load Duration: ' + CAST(DATEDIFF(second,@batch_start_time,@batch_end_time) AS NVARCHAR(20)) + 'seconds';
	PRINT '====================================================================';
	END TRY

	BEGIN CATCH
	PRINT '====================================================================';
	PRINT 'Error Occured During Loading Bronze Layer';
	PRINT 'Error Message' + ERROR_MESSAGE();
	PRINT 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
	PRINT 'Error Message' + CAST(ERROR_STATE() AS NVARCHAR);
	PRINT '===================================================================='; 
	END CATCH
END

EXEC bronze.load_bronze;

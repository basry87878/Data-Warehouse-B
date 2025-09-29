CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME;
	BEGIN TRY
		PRINT '===============================================================';
		PRINT 'LOADING BRONZE LAYER';
		PRINT '===============================================================';
		SET @start_time = GETDATE();

		PRINT '>> TRUNCATING TABLES';
		TRUNCATE TABLE bronze.crm_cst_info;
		TRUNCATE TABLE bronze.crm_prd_info;
		TRUNCATE TABLE bronze.crm_sales_details;
		TRUNCATE TABLE bronze.erp_CUST_AZ12;
		TRUNCATE TABLE bronze.erp_LOC_A101;
		TRUNCATE TABLE bronze.erp_PX_CAT_G1V2;


		PRINT '---------------------------------------------------------------';
		PRINT 'LOADING CRM TABLES';
		PRINT '---------------------------------------------------------------';
		
		
		BULK INSERT bronze.crm_cst_info
		FROM 'E:\Work\Data Engineering\SQL Data Warehouse\sql-data-warehouse-project\datasets\source_crm\cust_info.CSV'
		WITH ( 
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		

		BULK INSERT bronze.crm_prd_info
		FROM 'E:\Work\Data Engineering\SQL Data Warehouse\sql-data-warehouse-project\datasets\source_crm\prd_info.CSV'
		WITH ( 
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);


		BULK INSERT bronze.crm_sales_details
		FROM 'E:\Work\Data Engineering\SQL Data Warehouse\sql-data-warehouse-project\datasets\source_crm\sales_details.CSV'
		WITH ( 
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		PRINT '---------------------------------------------------------------';
		PRINT 'LOADING ERP TABLES';
		PRINT '---------------------------------------------------------------';
		BULK INSERT bronze.erp_CUST_AZ12
		FROM 'E:\Work\Data Engineering\SQL Data Warehouse\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.CSV'
		WITH ( 
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);


		BULK INSERT bronze.erp_LOC_A101
		FROM 'E:\Work\Data Engineering\SQL Data Warehouse\sql-data-warehouse-project\datasets\source_erp\LOC_A101.CSV'
		WITH ( 
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);


		BULK INSERT bronze.erp_PX_CAT_G1V2
		FROM 'E:\Work\Data Engineering\SQL Data Warehouse\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.CSV'
		WITH ( 
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SET @end_time = GETDATE();
		PRINT '>> load Duration: ' + CAST (DATEDIFF(second, @end_time , @start_time) AS NVARCHAR) + ' Seconds';
	END TRY
	BEGIN CATCH
		PRINT '===============================================================';
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST(ERROR_STATE() AS NVARCHAR); 
		PRINT '===============================================================';
	END CATCH
END 

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
USE DATAWAREHOUSE;
USE DATAWAREHOUSE.BRONZE;


CREATE OR REPLACE STAGE DATAWAREHOUSE.BRONZE.BRONZE_STAGE;


CREATE OR REPLACE FILE FORMAT DATAWAREHOUSE.BRONZE.MY_CSV_FORMAT
  TYPE = CSV
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1;


CREATE OR REPLACE PROCEDURE DATAWAREHOUSE.BRONZE.LOAD_BRONZE()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
    var start_time = new Date();
    var commands = [
        -- CRM tables
        "TRUNCATE TABLE DATAWAREHOUSE.BRONZE.CRM_CUST_INFO",
        `COPY INTO DATAWAREHOUSE.BRONZE.CRM_CUST_INFO 
            FROM @DATAWAREHOUSE.BRONZE.BRONZE_STAGE/cust_info.csv 
            FILE_FORMAT = (FORMAT_NAME = DATAWAREHOUSE.BRONZE.MY_CSV_FORMAT)`,

        "TRUNCATE TABLE DATAWAREHOUSE.BRONZE.CRM_PRD_INFO",
        `COPY INTO DATAWAREHOUSE.BRONZE.CRM_PRD_INFO 
            FROM @DATAWAREHOUSE.BRONZE.BRONZE_STAGE/prd_info.csv 
            FILE_FORMAT = (FORMAT_NAME = DATAWAREHOUSE.BRONZE.MY_CSV_FORMAT)`,

        "TRUNCATE TABLE DATAWAREHOUSE.BRONZE.CRM_SALES_DETAILS",
        `COPY INTO DATAWAREHOUSE.BRONZE.CRM_SALES_DETAILS 
            FROM @DATAWAREHOUSE.BRONZE.BRONZE_STAGE/sales_details.csv 
            FILE_FORMAT = (FORMAT_NAME = DATAWAREHOUSE.BRONZE.MY_CSV_FORMAT)`,

        -- ERP tables
        "TRUNCATE TABLE DATAWAREHOUSE.BRONZE.ERP_LOC_A101",
        `COPY INTO DATAWAREHOUSE.BRONZE.ERP_LOC_A101 
            FROM @DATAWAREHOUSE.BRONZE.BRONZE_STAGE/loc_a101.csv 
            FILE_FORMAT = (FORMAT_NAME = DATAWAREHOUSE.BRONZE.MY_CSV_FORMAT)`,

        "TRUNCATE TABLE DATAWAREHOUSE.BRONZE.ERP_CUST_AZ12",
        `COPY INTO DATAWAREHOUSE.BRONZE.ERP_CUST_AZ12 
            FROM @DATAWAREHOUSE.BRONZE.BRONZE_STAGE/cust_az12.csv 
            FILE_FORMAT = (FORMAT_NAME = DATAWAREHOUSE.BRONZE.MY_CSV_FORMAT)`,

        "TRUNCATE TABLE DATAWAREHOUSE.BRONZE.ERP_PX_CAT_G1V2",
        `COPY INTO DATAWAREHOUSE.BRONZE.ERP_PX_CAT_G1V2 
            FROM @DATAWAREHOUSE.BRONZE.BRONZE_STAGE/px_cat_g1v2.csv 
            FILE_FORMAT = (FORMAT_NAME = DATAWAREHOUSE.BRONZE.MY_CSV_FORMAT)`
    ];

    try {
        for (var i = 0; i < commands.length; i++) {
            var stmt = snowflake.createStatement({sqlText: commands[i]});
            stmt.execute();
        }
        var end_time = new Date();
        var duration = (end_time - start_time) / 1000; -- seconds
        return "✅ Bronze layer load completed successfully in " + duration + " seconds.";
    } catch (err) {
        return "❌ ERROR: " + err.message;
    }
$$;

-- 5. Call the procedure
CALL DATAWAREHOUSE.BRONZE.LOAD_BRONZE();

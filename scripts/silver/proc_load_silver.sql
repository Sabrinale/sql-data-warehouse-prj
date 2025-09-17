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
-- Enable Snowflake Scripting
CREATE OR REPLACE PROCEDURE silver.load_silver()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE 
    start_time      TIMESTAMP_NTZ;
    end_time        TIMESTAMP_NTZ;
    batch_start_time TIMESTAMP_NTZ;
    batch_end_time  TIMESTAMP_NTZ;
BEGIN
    LET batch_start_time = CURRENT_TIMESTAMP;
    RETURN '================================================\nLoading Silver Layer\n================================================';

    -----------------------------
    -- Loading CRM Tables
    -----------------------------

    -- Loading silver.crm_cust_info
    LET start_time = CURRENT_TIMESTAMP;
    TRUNCATE TABLE silver.crm_cust_info;
    INSERT INTO silver.crm_cust_info (
        cst_id, 
        cst_key, 
        cst_firstname, 
        cst_lastname, 
        cst_marital_status, 
        cst_gndr,
        cst_create_date
    )
    SELECT
        cst_id,
        cst_key,
        TRIM(cst_firstname) AS cst_firstname,
        TRIM(cst_lastname) AS cst_lastname,
        CASE 
            WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
            WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
            ELSE 'n/a'
        END AS cst_marital_status,
        CASE 
            WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
            WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
            ELSE 'n/a'
        END AS cst_gndr,
        cst_create_date
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
        FROM bronze.crm_cust_info
        WHERE cst_id IS NOT NULL
    ) t
    WHERE flag_last = 1;

    -----------------------------
    -- Loading silver.crm_prd_info
    -----------------------------
    LET start_time = CURRENT_TIMESTAMP;
    TRUNCATE TABLE silver.crm_prd_info;
    INSERT INTO silver.crm_prd_info (
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
        REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
        SUBSTRING(prd_key,7,LENGTH(prd_key)) AS prd_key,
        prd_nm,
        COALESCE(prd_cost,0) AS prd_cost,
        CASE 
            WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
            WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
            WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
            WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
            ELSE 'n/a'
        END AS prd_line,
        CAST(prd_start_dt AS DATE) AS prd_start_dt,
        CAST(
            LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - INTERVAL '1 DAY' 
            AS DATE
        ) AS prd_end_dt
    FROM bronze.crm_prd_info;

    -----------------------------
    -- Loading silver.crm_sales_details
    -----------------------------
    LET start_time = CURRENT_TIMESTAMP;
    TRUNCATE TABLE silver.crm_sales_details;
    INSERT INTO silver.crm_sales_details (
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
        IFF(sls_order_dt=0 OR LENGTH(sls_order_dt)!=8,NULL,TO_DATE(sls_order_dt,'YYYYMMDD')) AS sls_order_dt,
        IFF(sls_ship_dt=0 OR LENGTH(sls_ship_dt)!=8,NULL,TO_DATE(sls_ship_dt,'YYYYMMDD')) AS sls_ship_dt,
        IFF(sls_due_dt=0 OR LENGTH(sls_due_dt)!=8,NULL,TO_DATE(sls_due_dt,'YYYYMMDD')) AS sls_due_dt,
        IFF(sls_sales IS NULL OR sls_sales<=0 OR sls_sales != sls_quantity*ABS(sls_price),
            sls_quantity*ABS(sls_price),
            sls_sales) AS sls_sales,
        sls_quantity,
        IFF(sls_price IS NULL OR sls_price<=0,
            sls_sales/NULLIF(sls_quantity,0),
            sls_price) AS sls_price
    FROM bronze.crm_sales_details;

    -----------------------------
    -- Loading silver.erp_cust_az12
    -----------------------------
    LET start_time = CURRENT_TIMESTAMP;
    TRUNCATE TABLE silver.erp_cust_az12;
    INSERT INTO silver.erp_cust_az12 (
        cid,
        bdate,
        gen
    )
    SELECT
        IFF(cid LIKE 'NAS%',SUBSTRING(cid,4,LENGTH(cid)),cid) AS cid,
        IFF(bdate > CURRENT_DATE,NULL,bdate) AS bdate,
        CASE 
            WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
            WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
            ELSE 'n/a'
        END AS gen
    FROM bronze.erp_cust_az12;

    -----------------------------
    -- Loading silver.erp_loc_a101
    -----------------------------
    LET start_time = CURRENT_TIMESTAMP;
    TRUNCATE TABLE silver.erp_loc_a101;
    INSERT INTO silver.erp_loc_a101 (
        cid,
        cntry
    )
    SELECT
        REPLACE(cid,'-','') AS cid,
        CASE
            WHEN TRIM(cntry)='DE' THEN 'Germany'
            WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
            WHEN TRIM(cntry)='' OR cntry IS NULL THEN 'n/a'
            ELSE TRIM(cntry)
        END AS cntry
    FROM bronze.erp_loc_a101;

    -----------------------------
    -- Loading silver.erp_px_cat_g1v2
    -----------------------------
    LET start_time = CURRENT_TIMESTAMP;
    TRUNCATE TABLE silver.erp_px_cat_g1v2;
    INSERT INTO silver.erp_px_cat_g1v2 (
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

    -----------------------------
    -- End
    -----------------------------
    LET batch_end_time = CURRENT_TIMESTAMP;
    RETURN 'Loading Silver Layer Completed. Total Duration: ' || DATEDIFF('second', batch_start_time, batch_end_time) || ' seconds';
    
EXCEPTION
    WHEN OTHER THEN
        RETURN 'Error Occurred During Loading Silver Layer: ' || ERROR_MESSAGE();
END;
$$;

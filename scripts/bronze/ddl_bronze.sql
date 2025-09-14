/*
===============================================================================
DDL Script: Create Bronze Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'bronze' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/
-- Drop tables if they exist
DROP TABLE IF EXISTS bronze.crm_cust_info;
DROP TABLE IF EXISTS bronze.crm_prd_info;
DROP TABLE IF EXISTS bronze.crm_sales_details;
DROP TABLE IF EXISTS bronze.erp_loc_a101;
DROP TABLE IF EXISTS bronze.erp_cust_az12;
DROP TABLE IF EXISTS bronze.erp_px_cat_g1v2;

-- Create table: crm_cust_info
CREATE TABLE bronze.crm_cust_info (
    cst_id              INT,
    cst_key             STRING(50),
    cst_firstname       STRING(50),
    cst_lastname        STRING(50),
    cst_marital_status  STRING(50),
    cst_gndr            STRING(50),
    cst_create_date     DATE
);

-- Create table: crm_prd_info
CREATE TABLE bronze.crm_prd_info (
    prd_id       INT,
    prd_key      STRING(50),
    prd_nm       STRING(50),
    prd_cost     INT,
    prd_line     STRING(50),
    prd_start_dt TIMESTAMP_NTZ,
    prd_end_dt   TIMESTAMP_NTZ
);

-- Create table: crm_sales_details
CREATE TABLE bronze.crm_sales_details (
    sls_ord_num  STRING(50),
    sls_prd_key  STRING(50),
    sls_cust_id  INT,
    sls_order_dt INT,
    sls_ship_dt  INT,
    sls_due_dt   INT,
    sls_sales    INT,
    sls_quantity INT,
    sls_price    INT
);

-- Create table: erp_loc_a101
CREATE TABLE bronze.erp_loc_a101 (
    cid    STRING(50),
    cntry  STRING(50)
);

-- Create table: erp_cust_az12
CREATE TABLE bronze.erp_cust_az12 (
    cid    STRING(50),
    bdate  DATE,
    gen    STRING(50)
);

-- Create table: erp_px_cat_g1v2
CREATE TABLE bronze.erp_px_cat_g1v2 (
    id           STRING(50),
    cat          STRING(50),
    subcat       STRING(50),
    maintenance  STRING(50)
);

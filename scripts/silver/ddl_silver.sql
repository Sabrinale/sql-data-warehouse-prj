/*
===============================================================================
DDL Script: Create silver Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'silver' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'silver' Tables
===============================================================================
*/
USE DATABASE DATAWAREHOUSE;
USE SCHEMA SILVER;

//CREATE OR REPLACE SCHEMA SILVER CLONE BRONZE;

-- Drop tables if they exist
DROP TABLE IF EXISTS silver.crm_cust_info;
DROP TABLE IF EXISTS silver.crm_prd_info;
DROP TABLE IF EXISTS silver.crm_sales_details;
DROP TABLE IF EXISTS silver.erp_loc_a101;
DROP TABLE IF EXISTS silver.erp_cust_az12;
DROP TABLE IF EXISTS silver.erp_px_cat_g1v2;

-- Create table: crm_cust_info
CREATE TABLE silver.crm_cust_info (
    cst_id              INT,
    cst_key             STRING(50),
    cst_firstname       STRING(50),
    cst_lastname        STRING(50),
    cst_marital_status  STRING(50),
    cst_gndr            STRING(50),
    cst_create_date     DATE,
    hwh_create_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Create table: crm_prd_info
CREATE TABLE silver.crm_prd_info (
    prd_id       INT,
    prd_key      STRING(50),
    prd_nm       STRING(50),
    prd_cost     INT,
    prd_line     STRING(50),
    prd_start_dt TIMESTAMP_NTZ,
    prd_end_dt   TIMESTAMP_NTZ,
    hwh_create_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Create table: crm_sales_details
CREATE TABLE silver.crm_sales_details (
    sls_ord_num  STRING(50),
    sls_prd_key  STRING(50),
    sls_cust_id  INT,
    sls_order_dt INT,
    sls_ship_dt  INT,
    sls_due_dt   INT,
    sls_sales    INT,
    sls_quantity INT,
    sls_price    INT,    
    hwh_create_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Create table: erp_loc_a101
CREATE TABLE silver.erp_loc_a101 (
    cid    STRING(50),
    cntry  STRING(50),    
    hwh_create_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Create table: erp_cust_az12
CREATE TABLE silver.erp_cust_az12 (
    cid    STRING(50),
    bdate  DATE,
    gen    STRING(50),
    hwh_create_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Create table: erp_px_cat_g1v2
CREATE TABLE silver.erp_px_cat_g1v2 (
    id           STRING(50),
    cat          STRING(50),
    subcat       STRING(50),
    maintenance  STRING(50),
    hwh_create_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

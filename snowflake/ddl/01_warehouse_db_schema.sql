-- 01_warehouse_db_schema.sql
-- Sets up the Snowflake objects the rest of the DDL/DML depend on.
-- Run once, as a role with CREATE privileges (e.g. SYSADMIN).

CREATE WAREHOUSE IF NOT EXISTS ecommerce_wh
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;

CREATE DATABASE IF NOT EXISTS ecommerce_db;
CREATE SCHEMA IF NOT EXISTS ecommerce_db.analytics;

USE WAREHOUSE ecommerce_wh;
USE DATABASE ecommerce_db;
USE SCHEMA analytics;

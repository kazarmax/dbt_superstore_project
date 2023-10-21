-- SQL scripts to prepare warehouse in Snowflake and load the source data files there

-- In Snowflake account: create a new virtual warehouse, two new databases 
-- (one for raw data, the other for future dbt development), and one new schema 
create warehouse wh_superstore;
create database raw;
create database analytics;
create schema raw.superstore;

-- Creating stage in Snowflake to store source data files
USE DATABASE raw;
USE SCHEMA superstore;
CREATE STAGE file_stage;

-- Load files through Snowflake interface

-- Creating tables for raw data and filling them in

-- Creating 'orders' table 
CREATE TABLE raw.superstore.orders(
   row_id        INTEGER  NOT NULL PRIMARY KEY 
  ,order_id      VARCHAR(14) NOT NULL
  ,order_date    DATE  NOT NULL
  ,ship_date     DATE  NOT NULL
  ,ship_mode     VARCHAR(14) NOT NULL
  ,customer_id   VARCHAR(8) NOT NULL
  ,customer_name VARCHAR(22) NOT NULL
  ,segment       VARCHAR(11) NOT NULL
  ,country       VARCHAR(13) NOT NULL
  ,city          VARCHAR(17) NOT NULL
  ,state         VARCHAR(20) NOT NULL
  ,postal_code   VARCHAR(50)
  ,region        VARCHAR(7) NOT NULL
  ,product_id    VARCHAR(15) NOT NULL
  ,category      VARCHAR(15) NOT NULL
  ,subcategory   VARCHAR(11) NOT NULL
  ,product_name  VARCHAR(127) NOT NULL
  ,sales         NUMERIC(9,4) NOT NULL
  ,quantity      INTEGER  NOT NULL
  ,discount      NUMERIC(4,2) NOT NULL
  ,profit        NUMERIC(21,16) NOT NULL
);

-- Loading data into the 'orders' table 
copy into raw.superstore.orders
from @file_stage/supestore_orders.csv
file_format = (
    type = 'CSV'
    field_delimiter = '|'
    skip_header = 1
    ); 

-- Creating 'people' table 
CREATE TABLE raw.superstore.sales_managers(
   person VARCHAR(17) NOT NULL PRIMARY KEY
  ,region VARCHAR(7) NOT NULL
);

-- Loading data into the 'sales_managers' table
copy into raw.superstore.sales_managers
from @file_stage/supestore_people.csv
file_format = (
    type = 'CSV'
    field_delimiter = '|'
    skip_header = 1
    ); 

-- Creating 'returned_orders' table 
CREATE TABLE raw.superstore.returned_orders(
   returned   Boolean NOT NULL
  ,order_id   VARCHAR(14) NOT NULL
);

-- Loading data into the 'returned_orders' table
copy into raw.superstore.returned_orders
from @file_stage/supestore_returns.csv
file_format = (
    type = 'CSV'
    field_delimiter = '|'
    skip_header = 1
    );
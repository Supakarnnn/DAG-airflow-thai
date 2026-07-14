\getenv warehouse_password WAREHOUSE_PASSWORD
CREATE USER warehouse WITH PASSWORD :'warehouse_password';
CREATE DATABASE warehouse OWNER warehouse;

\connect warehouse
CREATE SCHEMA IF NOT EXISTS bronze AUTHORIZATION warehouse;
CREATE SCHEMA IF NOT EXISTS silver AUTHORIZATION warehouse;
CREATE SCHEMA IF NOT EXISTS gold   AUTHORIZATION warehouse;

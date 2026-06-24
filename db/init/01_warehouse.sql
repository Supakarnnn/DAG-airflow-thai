-- รันครั้งเดียวตอน postgres สร้าง volume ใหม่: แยก db warehouse ออกจาก metadata ของ airflow
CREATE USER warehouse WITH PASSWORD 'warehouse';
CREATE DATABASE warehouse OWNER warehouse;

\connect warehouse
CREATE SCHEMA IF NOT EXISTS bronze AUTHORIZATION warehouse;
CREATE SCHEMA IF NOT EXISTS silver AUTHORIZATION warehouse;
CREATE SCHEMA IF NOT EXISTS gold   AUTHORIZATION warehouse;

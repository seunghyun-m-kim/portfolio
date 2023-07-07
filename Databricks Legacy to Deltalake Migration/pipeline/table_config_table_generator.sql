-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC #### Initial Configuration

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### Create Table

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS config;

CREATE OR REPLACE TABLE config.table_config
  (vendor STRING, database STRING, table STRING, port INT, primary_keys STRING, alternate_keys STRING, checksum_column STRING, secrets_scope STRING)
LOCATION '/mnt/mike/tables/databricks/config/table_config';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### Modify Table

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC INSERT INTO config.table_config VALUES ('mysql', "HR", 'regions', 3306, "['region_id']", "['region_name']", 'region_name', 'mysql_3306');
-- MAGIC INSERT INTO config.table_config VALUES ('mysql', "HR", 'countries', 3306, "['country_id']", "['country_name', 'region_id']", 'country_name', 'mysql_3306');
-- MAGIC INSERT INTO config.table_config VALUES ('mysql', "HR", 'locations', 3306, "['location_id']", "['street_address', 'postal_code', 'city', 'state_province', 'country_id']", 'state_province', 'mysql_3306');
-- MAGIC INSERT INTO config.table_config VALUES ('mysql', "HR", 'jobs', 3306, "['job_id']", "['job_title', 'min_salary', 'max_salary']", 'job_title', 'mysql_3306');
-- MAGIC INSERT INTO config.table_config VALUES ('mysql', "HR", 'departments', 3306, "['department_id']", "['department_name', 'location_id']", 'department_name', 'mysql_3306');
-- MAGIC INSERT INTO config.table_config VALUES ('mysql', "HR", 'employees', 3306, "['employee_id']", "['first_name', 'last_name', 'email', 'phone_number', 'hire_date', 'job_id', 'salary', 'manager_id', 'department_id']", 'email', 'mysql_3306');
-- MAGIC INSERT INTO config.table_config VALUES ('mysql', "HR", 'dependents', 3306, "['dependent_id']", "['first_name', 'last_name', 'relationship', 'employee_id']", 'first_name', 'mysql_3306');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### Check Table

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC SELECT *
-- MAGIC FROM config.table_config

# Databricks Legacy Migration

This project was completed for a government database modernization contract bid from FEMA.

---

In this project, we aim to demonstrate a pipeline that extracts from a legacy database (MySQL, Postgres, SQL Server) and loads it into the Databricks Delta Lake Bronze layer. An accompanying [Github Actions CI/CD pipeline](https://github.com/seunghyun-m-kim/portfolio/tree/master/.github/workflows) runs an automated integration test upon pull request prior to merging branches.  

---
### Files

- `legacy_to_db_pipeline.py` : Main pipeline script.
- `initial_configuration.py` : Helper script that generates necessary variables to be used in the main pipeline script by triangulating the configuration table (table_config).
- `table_config_table_generator.sql` : Helper script that creates or edits the configuration table (table_config).
- `test_validate.py` : Test script that validates whether the legacy data was extracted and loaded properly.

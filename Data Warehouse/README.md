# Implementation of Data Warehouse and ETL Pipeline with AWS Redshift
### Project: Data Warehouse

This project was completed as part of Udacity's Data Engineering Nanodegree. Data set and prompt provided by [2011–2020 Udacity, Inc.](https://www.udacity.com), used under [CC BY](https://creativecommons.org/licenses/by-nc-nd/3.0/).

---

In this project, we continue to assist the startup company Sparkify, this time by moving their music data to the cloud and recreating the star schema on the Amazon Redshift data warehouse. Their data now resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

Our goal is to build an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables to be used in the company's data analytics.

The following is the star schema for their data:

**Fact Table**

1. **songplays**

    - songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

**Dimension Tables**

2. **users**
    - user_id, first_name, last_name, gender, level

3. **songs**
    - song_id, title, artist_id, year, duration

4. **artists**
    - artist_id, name, location, latitude, longitude

5. **time**
    - start_time, hour, day, week, month, year, weekday

---
### Install

- This project requires Python 3 (project was built on version 3.6.3) with `configparser` and `psycopg2` libraries installed.
- AWS account is required.

---
### Run

- Clone this repo to your computer.
- Access AWS and launch a Redshift cluster (dc2.large/4 nodes) with the following features:
    1. Security group with *Custom TCP Rule* Type, *TCP* Protocol, *5439* Port, and your custom IP address.    
    2. IAM role for use case, *Redshift - Customizable* with *AmazonS3ReadOnlyAccess* enabled.
    3. Database name, user, password, and port according to the *dwh.cfg* file.
- Once the cluster is fully launched, go to the *Properties* tab and copy the cluster's *Endpoint* and *IAM role arn*. Open *dwh.cfg* file in the repo and paste the *Endpoint* (minus :5439 at the end) next to "Host=" under [Cluster] and the *IAM role arn* next to "ARN=" under [IAM_ROLE].
- In a terminal or command window, navigate to the top-level repo directory.
- Run `python create_tables.py`.
- Run `python etl.py`.
- You can check the results on the Query Editor in the AWS Redshift console.

---
### Files

- `dwh.cfg` : Preference file that stores settings and configuration information.
- `create_tables.py` : Command script dropping (if required) and creating empty tables inside the Redshift data warehouse.
- `etl.py` : Command script extracting data from JSON files and inserting them into the created tables.
- `sql_queries.py` : Support script containing quries to drop tables, create tables, and insert data.

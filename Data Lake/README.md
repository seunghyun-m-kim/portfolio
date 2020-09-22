# Implementation of AWS Data Lake and ETL Pipeline with Spark
### Project: Data Lake

This project was completed as part of Udacity's Data Engineering Nanodegree. Data set and prompt provided by [2011–2020 Udacity, Inc.](https://www.udacity.com), used under [CC BY](https://creativecommons.org/licenses/by-nc-nd/3.0/).

---

In this project, we continue to assist the startup company Sparkify, this time by shifting their data warehouse to data lake. Their data still resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

Our goal is to build an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables to be used in the company's data analytics.

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

- This project requires Python 3 (project was built on version 3.6.3).
- AWS account is required.

---
### Run

- Clone this repo to your computer.
- Access AWS.
- Create an EC2 key pair with a unique name and save the `.pem` file to your computer. Open a terminal and restrict the `.pem` file access by entering in the following: `sudo chmod 600 <pem directory>` (see below for reference) [[Reference](https://stackabuse.com/how-to-fix-warning-unprotected-private-key-file-on-mac-and-linux/)]
> `sudo chmod 600 ~/key.pem`
- Launch an EMR Spark cluster with the following features (leave all others as default):
    1. Release: `emr-5.30.1`
    2. Applications: `Spark: Spark 2.4.5 on Hadoop 2.8.5 YARN and Zeppelin 0.8.2`
    3. Instance type: `m5.xlarge`
    4. Number of instances: `3`
    5. EC2 key pair: `your created EC2 key pair`
- Open the `dl.cfg` file and fill in the AWS access keys.
- Open the `etl.py` file and next to `output_data=`, fill in your S3 directory.
- Once the cluster is fully launched (status: Waiting), copy the **Master public DNS** from the cluster main page.    
- Open a terminal and enter the following: `scp -i <pem directory> <etl.py directory> hadoop@<Master public DNS>:~/` (see below for reference). This will copy the `etl.py` file to the EMR cluster.
> `scp -i ~/clust.pem ~/etl.py hadoop@ec2-34-215-249-113.us-west-2.compute.amazonaws.com:~/`
- Repeat the above step, but this time for `dl.cfg` file.
- On the cluster main page, click the `SSH` hyperlink next to **Master public DNS** and copy the shown command. Fix the pem directory as prompted and enter the command on the terminal. This will connect your shell to cluster.
- Change the Python environment to Python 3 by the following command (default is Python 2, which lacks script modules): `sudo sed -i -e '$a\export PYSPARK_PYTHON=/usr/bin/python3' /etc/spark/conf/spark-env.sh` [[Reference](https://aws.amazon.com/premiumsupport/knowledge-center/emr-pyspark-python-3x/)]
- Locate spark-submit program by entering `which spark-submit`, then copy the directory (usually `/usr/bin/spark-submit`).
- Enter the following command: `<spark-submit directory> --master yarn <etl.py directory>` (see below for reference). This will run the pipeline.
> `/usr/bin/spark-submit --master yarn ~/etl.py`
- You can check the results by accessing your S3.
---
### Files

- `dl.cfg` : Preference file that stores AWS access keys.
- `etl.py` : Command script extracting data from JSON files, transforming them, and loading them into CSV files.

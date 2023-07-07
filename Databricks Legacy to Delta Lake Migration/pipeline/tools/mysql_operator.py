# Databricks notebook source
import mysql.connector

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### MySQL Query

# COMMAND ----------

query1 = '''
UPDATE employees
SET job_title = 'Public Accountant'
WHERE job_id = 1;
'''

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### MySQL Connection

# COMMAND ----------

mydb = mysql.connector.connect(
  host='',
  user='',
  password='',
  database=''
)

mycursor = mydb.cursor()

mycursor.execute(query1)

mydb.commit()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Close MySQL Connection

# COMMAND ----------

mycursor.close()
mydb.close()

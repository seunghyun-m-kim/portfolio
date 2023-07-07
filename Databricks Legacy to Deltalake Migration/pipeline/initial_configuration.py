# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC #### Installation and Setting 

# COMMAND ----------

from pyspark.sql.functions import *
spark.conf.set('spark.sql.session.timeZone', 'America/New_York')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Variable Assignment

# COMMAND ----------

vendor = dbutils.widgets.get("vendor")
database = dbutils.widgets.get("database")
table = dbutils.widgets.get("table")

# COMMAND ----------

table_config_context = spark.sql(f'''SELECT * 
                                     FROM config.table_config 
                                     WHERE vendor = '{vendor}' AND 
                                           database = '{database}' AND 
                                           table = '{table}' 
                                   ''').collect()[0]
                                  
port = table_config_context['port']
primary_keys = table_config_context['primary_keys'] 
alternate_keys = table_config_context['alternate_keys']
checksum_column = table_config_context['checksum_column']
secrets_scope = table_config_context['secrets_scope']

# COMMAND ----------

upsert_condition1 = ''

for i in eval(primary_keys):
    upsert_condition1 += f' AND a.{i} <=> b.{i}'

upsert_condition1 = upsert_condition1[5:]

# COMMAND ----------

upsert_condition2 = ''

for i in eval(alternate_keys):
    upsert_condition2 += f' OR a.{i} <=> b.{i}'

upsert_condition2 = upsert_condition2[4:]

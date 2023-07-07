# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC #### Initial Configuration

# COMMAND ----------

dbutils.widgets.text("vendor", "mysql")
dbutils.widgets.text("database", "HR")
dbutils.widgets.text("table", "regions")

# COMMAND ----------

# MAGIC %run ./initial_configuration

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Data Testing/Validation
# MAGIC
# MAGIC > Delta table data will be tested and validated against the initial legacy dataframe to ensure the table was loaded correctly. Row count, column count, and checksum (CRC32) of a string column will be used.

# COMMAND ----------

legacy_df = (spark.read.format(f'{vendor}')
                      .option("dbtable", f'{table}')
                      .option("host", dbutils.secrets.get(scope = f'{secrets_scope}', key = "host"))
                      .option("port", f'{port}')
                      .option("database", f'{database}')
                      .option("user", dbutils.secrets.get(scope = f'{secrets_scope}', key = "user"))
                      .option("password", dbutils.secrets.get(scope = f'{secrets_scope}', key = "password"))
                      .load()
                      .withColumn("processed", current_timestamp())
            )

# COMMAND ----------

try:
    assert legacy_df.count() == spark.sql(f'SELECT * FROM {database}.{table}').count()
    print('Row count is identical and validated.')

except:
    raise Exception('Row count is not identical. Check issue and rerun the pipeline.')

try:
    assert len(legacy_df.collect()[0]) == len(spark.sql(f'SELECT * FROM {database}.{table}').collect()[0])
    print('Column count is identical and validated.')

except:
    raise Exception('Column count is not identical. Check issue and rerun the pipeline.')

try:
    legacy_df.createOrReplaceTempView('legacy_df')
    assert (spark.sql(f'SELECT SUM(CRC32({checksum_column})) total FROM legacy_df').collect()[0]['total'] == 
            spark.sql(f'SELECT SUM(CRC32({checksum_column})) total FROM {database}.{table}').collect()[0]['total'])
    print('CRC32 checksum is identical and validated.')

except:
    raise Exception('Checksum is not identical. Check issue and rerun the pipeline.')

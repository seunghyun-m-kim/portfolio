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
# MAGIC #### Pipeline
# MAGIC
# MAGIC > Pipeline will initially create a dataframe from the legacy database. If the Delta table does not exist, it will write the dataframe to Delta Lake (Bronze Layer). If the table does exist, it will execute upsert. 

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

spark.sql(f'''CREATE SCHEMA IF NOT EXISTS {database}''')

if not spark.catalog.tableExists(f'{database}.{table}'):
    legacy_df.write.mode('overwrite').option('path', f'/mnt/mike/tables/{vendor}/{database}/{table}').saveAsTable(f'{database}.{table}')

else:
    legacy_df.createOrReplaceTempView('legacy_df')

    spark.sql(f'''MERGE INTO {database}.{table} a
                 USING legacy_df b
                 ON {upsert_condition1}
                 WHEN MATCHED AND {upsert_condition2}
                    THEN UPDATE SET *
                 WHEN NOT MATCHED
                    THEN INSERT *
                 WHEN NOT MATCHED BY SOURCE
                    THEN DELETE 
              ''')

print(f'Table {database}.{table} loaded successfully.')
# Databricks notebook source
# MAGIC %md
# MAGIC ### Bronze to Silver (Transform Bronze Data)

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

raw_data_lake_schema = '''
  _row_id BIGINT,
  txn_date STRING,
  filler STRING,
  txn_time STRING,
  store_id STRING,
  terminal_id STRING,
  txn_id STRING,
  cust_id STRING,
  payment_mode STRING,
  partner_bank STRING,
  amount_paid DECIMAL(9,2),
  bank_payable DECIMAL(9,2),
  customer_payable DECIMAL(9,2),
  currency_code STRING,
  txn_status STRING
'''


# COMMAND ----------

df = spark.read.format("csv").schema(raw_data_lake_schema).option('header', True).load('/Volumes/mainframe_to_analytics/bronze/bronze/BRONZE20260104.CSV')

# COMMAND ----------

df.display(10)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Type Casting

# COMMAND ----------

df = df.withColumn(
    "txn_timestamp",
    to_timestamp(
        concat_ws(" ", col("txn_date"), col("txn_time")),
        "yyyy-MM-dd HH:mm:ss"
    )
)

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Normalize values
# MAGIC
# MAGIC For example : 'UPI   ' to 'UPI' 

# COMMAND ----------

cols_to_normalize = ['payment_mode', 'partner_bank', 'currency_code', 'terminal_id']

for col_ in cols_to_normalize:
    df = df.withColumn(col_, trim(col(col_)))

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Create FLAGS

# COMMAND ----------

df = df.withColumn('IS_CASH', when(((col('payment_mode') == 'UPI') | (col('payment_mode') == 'CARD')),'N').otherwise('Y'))

df = df.withColumn(
    "is_self_kiosk",
    when(col("terminal_id").isin("T01", "T02"), "N")
    .otherwise("Y")
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Write Data

# COMMAND ----------

df.write \
    .format('csv').option("header", "true").mode("overwrite").save("/Volumes/mainframe_to_analytics/silver/silver/silver.csv")

# COMMAND ----------


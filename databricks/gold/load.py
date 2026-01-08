# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Silver to Gold

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df = spark.read.format("csv").options(inferSchema='true').option('header', True).load('/Volumes/mainframe_to_analytics/silver/silver/silver.csv/')

# COMMAND ----------

df.display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Generating Aggregations

# COMMAND ----------

daily_store_sales = df.groupBy(to_date("txn_timestamp").alias("txn_date"), "store_id") \
    .agg(
        count("txn_id").alias("total_txns"),
        sum("amount_paid").alias("total_amount_paid"),
        sum("bank_payable").alias("total_bank_payable"),
        sum("customer_payable").alias("total_customer_payable")
    ).orderBy("txn_date", "store_id")


payment_mode_summary = df.groupBy("payment_mode") \
    .agg(
        count("txn_id").alias("total_txns"),
        sum("amount_paid").alias("total_amount")
    ).orderBy("payment_mode")


cash_summary = df.groupBy("IS_CASH") \
    .agg(
        count("txn_id").alias("total_txns"),
        sum("amount_paid").alias("total_amount")
    ).orderBy("IS_CASH")



kiosk_summary = df.groupBy("is_self_kiosk") \
    .agg(
        count("txn_id").alias("total_txns"),
        sum("amount_paid").alias("total_amount")
    ).orderBy("is_self_kiosk")



bank_summary = df.filter(col("partner_bank").isNotNull()) \
    .groupBy("partner_bank") \
    .agg(
        count("txn_id").alias("total_txns"),
        sum("bank_payable").alias("total_bank_payable")
    ).orderBy("partner_bank")



# COMMAND ----------

daily_store_sales.show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Create Delta Tables

# COMMAND ----------

daily_store_sales.write.format("delta").mode("overwrite") \
    .saveAsTable("mainframe_to_analytics.gold.daily_store_sales")

payment_mode_summary.write.format("delta").mode("overwrite") \
    .saveAsTable("mainframe_to_analytics.gold.payment_mode_summary")

cash_summary.write.format("delta").mode("overwrite") \
    .saveAsTable("mainframe_to_analytics.gold.cash_summary")

kiosk_summary.write.format("delta").mode("overwrite") \
    .saveAsTable("mainframe_to_analytics.gold.kiosk_summary")

bank_summary.write.format("delta").mode("overwrite") \
    .saveAsTable("mainframe_to_analytics.gold.bank_summary")

# COMMAND ----------


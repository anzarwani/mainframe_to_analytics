# Databricks notebook source
# MAGIC %md
# MAGIC ### Mainframe to Databricks - Ingest

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Utilizes copybook package to parse copybooks and accordingly generate field to field mappings
# MAGIC - will need to install it first

# COMMAND ----------

#!pip install copybook

# COMMAND ----------

import copybook
import pandas as pd
import os
from datetime import datetime

# COMMAND ----------

DATALAKE_COPYBOOK = """
              01  RAW-POS-RECORD.
    05 TXN-DATE            PIC X(10).
    05 FILLER              PIC X(1).

    05 TXN-TIME            PIC X(8).
    05 FILLER              PIC X(1).

    05 STORE-ID            PIC X(6).
    05 FILLER              PIC X(1).

    05 TERMINAL-ID         PIC X(4).
    05 FILLER              PIC X(1).

    05 TXN-ID              PIC X(12).
    05 FILLER              PIC X(1).

    05 CUST-ID             PIC X(10).
    05 FILLER              PIC X(1).

    05 PAYMENT-MODE        PIC X(10).
    05 FILLER              PIC X(1).

    05 PARTNER-BANK        PIC X(15).
    05 FILLER              PIC X(1).

    05 AMOUNT-PAID         PIC 9(7)V99.
    05 FILLER              PIC X(1).

    05 BANK-PAYABLE        PIC 9(7)V99.
    05 FILLER              PIC X(1).

    05 CUSTOMER-PAYABLE    PIC 9(7)V99.
    05 FILLER              PIC X(1).

    05 CURRENCY-CODE       PIC X(3).
    05 FILLER              PIC X(1).

    05 TXN-STATUS          PIC X(10).  

"""


RAW_POST_DATALAKE = "/Volumes/mainframe_to_analytics/mainframe_raw/pos_raw_data/RAW_POS_DATALAKE_20260104.TXT"

# COMMAND ----------

## For DataLake

## STEP 1 :  copybook -> get fields

# copybook also provides a parse_file method that receives a text filename
root = copybook.parse_string(DATALAKE_COPYBOOK)

# flatten returns a list of Fields and FieldGroups instead of traversing the tree
list_of_fields = root.flatten()

# COMMAND ----------

## STEP 2 : RAW MAINFRAME FILE -> RAW CSV FILE

parsed_rows = []

with open(RAW_POST_DATALAKE, "r") as f:
    for line in f:
        record = {}
        
        for field in list_of_fields:
            # only process Field objects, skip groups/fillers
            if isinstance(field, copybook.Field):
                str_field = line[field.start_pos : field.start_pos + field.get_total_length()]
                record[field.name.lower().replace("-", "_")] = field.parse(str_field)
        
        parsed_rows.append(record)

bronze_df = pd.DataFrame(parsed_rows)



# COMMAND ----------

bronze_df.head()

# COMMAND ----------

## STEP 3 : RAW CSV FILE -> CATALOG
BRONZE_DIR = "/Volumes/mainframe_to_analytics/bronze/bronze"
run_date = datetime.now().strftime("%Y%m%d")
BRONZE_FILE = os.path.join(
            BRONZE_DIR, f"BRONZE{run_date}.CSV"
        )

os.makedirs(BRONZE_DIR, exist_ok=True)

bronze_df.to_csv(BRONZE_FILE)

# COMMAND ----------


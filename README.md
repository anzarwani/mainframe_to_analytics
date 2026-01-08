# Departmental Store Mainframe Analytics POC

This project is a **proof-of-concept (POC)** for a small departmental store that uses POS (Point of Sale) machines to generate daily transactions.  

The transaction files are normally sent to a mainframe where **COBOL programs** run business logic. In this project, the mainframe logic has been **simulated using Python scripts**.  

The goal of this POC is to demonstrate how **mainframe data**, Python (PySpark, pandas, SQL), Databricks, and BI tools can be integrated to make mainframe data **analytics-ready**.

---

## Simulating a Mainframe Process Run

To simulate a mainframe process:

1. Run `pos_data_generator.py` – generates sample POS transactions.  
2. Run `mainframe_job.py` – applies business logic and produces raw mainframe files.

This creates a **raw mainframe file** which can then be used in a simple **ETL process** using the notebooks in the `databricks` folder.

---

## Databricks ETL

The notebooks in the `databricks` folder demonstrate **Bronze → Silver → Gold** processing:

- **Bronze:** Raw mainframe files ingested and parsed using copybook parser, and then saved using pandas to bronze.  
- **Silver:** Cleaned and typed data, enriched with business flags (like cash/self-kiosk).  
- **Gold:** Aggregated, analytics-ready **tables** persisted as Delta tables.  
  - Only in the **Gold layer** do we create Delta tables, which are ready for reporting and BI tools.

---

## Notes

- This POC is **not production-grade**, but demonstrates the workflow from **mainframe-style data → analytics-ready insights**.  
- The final Delta tables in Gold can be used for **dashboarding, reporting, or further analysis**.  

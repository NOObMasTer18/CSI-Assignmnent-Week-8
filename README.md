# NYC Taxi Data & JSON Flattening Pipeline

This repository contains two PySpark-based assignments:

---

## Assignment 1: NYC Taxi Dataset Analysis

**Problem Statement:**

- Load NYC taxi data to Data Lake / Blob Storage / Databricks and extract it as a DataFrame.
- Perform the following queries using PySpark:

### Queries

1. **Add Revenue Column**  
   Add a column named `Revenue` to the DataFrame, which is the sum of:
   - `fare_amount`
   - `extra`
   - `mta_tax`
   - `improvement_surcharge`
   - `tip_amount`
   - `tolls_amount`
   - `total_amount`

2. **Total Passengers by Area**  
   Calculate the total passengers in New York City, grouped by pickup area (`PULocationID`).

3. **Average Fare & Total Earning by Vendor**  
   Calculate real-time average fare and total earning amount for each vendor.

4. **Moving Count of Payments by Mode**  
   Compute the moving count of payments made by each payment mode.

5. **Top 2 Vendors by Revenue on a Specific Date**  
   Find the top two earning vendors on a specific date, showing number of passengers and total distance.

6. **Highest Passenger Route**  
   Identify the route (pickup and dropoff location pair) with the most passengers.

7. **Top Pickup Locations in Last 5/10 Seconds**  
   Get top pickup locations with most passengers in the last 5 or 10 seconds of trips.

---

## Assignment 2: JSON Flattening Pipeline

**Problem Statement:**

1. Load any dataset into DBFS (Databricks File System).
2. Flatten nested JSON fields using PySpark.
3. Write the flattened data as an external Parquet table.

---

## How to Run

1. **NYC Taxi Analysis**
   - Upload the NYC taxi dataset to your Databricks workspace or DataLake.
   - Run `NYC-Taxi Dataset Analysis.py` in a Spark-enabled Python environment.
   - Execute the script sequentially to perform all queries.

2. **JSON Flattening**
   - Upload your JSON dataset to DBFS.
   - Run `JSON_Flattening_Pipeline.py` in your Python environment.
   - The script will load, flatten, and export your data as a Parquet table.

---

## Requirements

- Databricks Community Edition or any Spark-enabled environment (Colab, EMR, etc.)
- PySpark
- Jupyter or Databricks Notebooks

---


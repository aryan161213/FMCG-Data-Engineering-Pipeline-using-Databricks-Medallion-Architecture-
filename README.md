#  FMCG Data Engineering Pipeline (Databricks | Delta Lake | S3)
## Overview

Built an end-to-end data engineering pipeline using **Databricks, PySpark, Delta Lake, and AWS S3**, implementing the **Medallion Architecture (Bronze → Silver → Gold)** with **incremental processing and MERGE-based upserts** for multi-source data.

The project simulates a real-world FMCG scenario involving:

*  Parent Company: **Atlikon**
*  Child Company: **Sports Bar**


##  Architecture

* Data ingested from **S3 landing layer**
* Processed through:

  * Bronze (raw data)
  * Silver (cleaned & transformed)
  * Gold (aggregated analytics)
* Final data stored in **Delta tables**


##  Key Features

###  Full Load Processing

* Initial historical data ingestion
* Built base datasets for analysis


###  Incremental Processing

* Processed only new incoming data
* Used **staging tables**
* Identified incremental months
* Avoided reprocessing full dataset


###  Upsert (MERGE) Logic

* Used **Delta Lake MERGE**
* Handled:

  * Updates
  * Inserts
* Ensured no duplicate records


###  Medallion Architecture

| Layer  | Description                    |
| ------ | -------------------------------|
| Bronze | Raw data from S3               |
| Silver | Cleaned and transformed data   |
| Gold   | Aggregated,business-ready data |


###  Multi-Source Data Consolidation

* Sports Bar data processed fully (Bronze → Silver → Gold)
* Atlikon parent data already available at Gold layer
* Final step:
   **Merged child data into parent Gold table**


###  Aggregation & Analytics

* Monthly aggregation
* KPIs:

  * Total Revenue
  * Total Quantity
  * Average Order Value (AOV)


###  Denormalized Data Model

* **Reduced query-time complexity** by creating a denormalized view with all relevant columns
* Enabled efficient dashboarding and faster analytics


###  File Lifecycle Management

Data flow:

```
Landing → Processing → Archived
```

* Used `dbutils.fs.mv()` to move processed files
* Prevented duplicate processing using MERGE-based upsert logic to ensure only new and updated records are written.


##  Tech Stack

* Apache Spark (PySpark)
* Delta Lake
* Databricks
* SQL
* AWS S3


##  Dashboard Insights

* Total Revenue & Quantity
* Average order value trends
* Top 10 Products
* Bottom 10 Products
* Revenue by Channel
* Time-based analysis


## Architecture Diagram

provided in the Images section


## 📸 Dashboard

provided in the Images section


##  Key Learnings

* Incremental pipeline design
* Delta Lake merge operations
* Data modeling (Fact & Dimension)
* Medallion architecture
* Real world ETL pipeline implementation


##  Author

Aryan Tambewagh

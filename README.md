#  FMCG Data Engineering Pipeline (Databricks | Delta Lake | S3)
## Overview

Built an end-to-end data engineering pipeline using **Databricks, PySpark, Delta Lake, and AWS S3**, implementing the **Medallion Architecture (Bronze → Silver → Gold)** with **incremental processing, MERGE-based upserts** and **regex-based data cleaning**, and a **BI-ready Star Schema** for **multi-source data consolidation**

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

## Business Logic
Integrated data from two different companies with conflicting schemas into a single source of truth.


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

###  Data Model

* Designed a Star Schema in the Gold layer (fact_orders + dim_customers, dim_products, dim_pricing, dim_date)
* Built a denormalized view joining all tables for high-speed BI dashboarding
* Consolidated child company (Sports Bar) Gold data into parent company (Atlikon) unified analytics table

###  Denormalized Data Model

* **Reduced query-time complexity** by creating a denormalized view with all relevant columns
* Enabled efficient dashboarding and faster analytics


###  File Lifecycle Management

Data flow:

```
Landing → Processing 
```

* Automated file movement using `dbutils.fs.mv()` for lifecycle management
* Leveraged Delta Lake (DeltaTable) to implement MERGE-based upserts, enabling incremental, idempotent pipelines that prevent duplicate processing by writing only new and updated record.

##  Tech Stack

* PySpark
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
<img width="20005" height="11129" alt="projectarchitecture" src="https://github.com/user-attachments/assets/98d295c1-435b-48b1-934a-26b46cc6b960" />






## Dashboard
<img width="1512" height="982" alt="Screenshot 2026-04-01 at 7 47 29 PM" src="https://github.com/user-attachments/assets/27467704-80fc-46f7-bc58-6a61eb464649" />



##  Key Learnings

* Incremental pipeline design
* Delta Lake merge operations
* Data modeling (Fact & Dimension)
* Medallion architecture
* Real world ETL pipeline implementation


##  Author

Aryan Tambewagh

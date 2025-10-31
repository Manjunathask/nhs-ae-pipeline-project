A&E Performance & Patient Flow Bottleneck Analysis

Project Objective: To build an automated, end-to-end BI solution that:

Ingests and simulates raw patient-level data into a Bronze Layer.

Cleans, transforms, and enriches this data in a Silver Layer.

Models the data into a Star Schema in a Gold Layer for analytics.

Identifies the "Decision to Admit" (DTA) bottleneck and correlates it with hospital-wide bed occupancy

Core Business Questions:

What is the average time spent in each stage of the A&E patient journey?

Which stage is the primary bottleneck?

Is there a statistical correlation between hospital-wide ward occupancy % and the DTA-to-Ward wait time?

How does our performance on the "4-Hour Target" change by time of day and day of week?

Technology Stack:

Architecture: Medallion Architecture (Bronze, Silver, Gold)

Database: SQL Server

Data Modeling: T-SQL (Stored Procedures, Schema Creation)

ETL: Python (Pandas, SQLAlchemy/pyodbc)

Orchestration: Apache Airflow

Visualization: Power BI (Power Query, DAX)

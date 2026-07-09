# Databricks_automated-timesheet-ingestion
Scalable PDF timesheet ETL pipeline with metadata tracking, data quality enforcement, and business intelligence dashboards.


Project Overview

>This project automates the ingestion and processing of employee timesheet PDF files into a structured data pipeline using Medallion Architecture (Bronze → Silver → Gold).
Key Features

Multi-format Conversion:
>Converts PDF timesheets into CSV, JSON, and XML formats.

Automated Metadata Tracking:
>Maintains a metadata table to track which PDF files have been processed and which are pending.

Date Extraction:
>Parses date information directly from source PDF filenames to generate target file names.

Layered Data Architecture:
Bronze Layer:
>Raw data loaded as-is from source files for auditability.

Silver Layer:
>Data cleansing and standardization (e.g., converting NULL values to -1), with data quality expectations. Bad records are isolated into a separate Silver error table.

Gold Layer:
>Aggregated business-ready tables:
>Monthly aggregated data
>Yearly aggregated data


Analytics & Visualization: Dashboards built on top of the Gold layer to analyze employee working hours by specific week.

Architecture Highlights

>All source PDFs have unique filenames.
>Date components are extracted from filenames for consistent target file naming.
>Robust error handling and data quality framework in the Silver layer.
>Designed for scalability and audit compliance.
>
>## Architecture

```mermaid
flowchart TD
    A[Source PDF Timesheets are uploaded into a volume] 
    --> B[PDF Parser & Converter created falt files]
    B --> C[CSV / JSON / XML Files are created in sepearte volume]
    C --> D[Metadata Table is updated to track pdf processing file status]
    D --> E[Bronze Layer - Raw Load - data is laoded as is from falt files using autoloader]
    E --> F[Silver Layer - Cleansing- Data is curated and handled expectations]
    F --> G[Gold Layer - Aggregates -aggregate tables are loaded ]
    G --> H[Dashboards  to track employee hours worked]


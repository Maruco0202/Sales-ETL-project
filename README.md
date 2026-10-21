# Sales ETL Project

## Overview

This project implements an end-to-end Data Engineering pipeline using PySpark and follows the Medallion Architecture pattern.

Pipeline Flow:

```text
Source CSV
    ↓
Bronze Layer
    ↓
Silver Validation
    ↓
Silver Cleaning
    ↓
Gold Sales
    ↓
Gold Customer
```

The solution applies software engineering and production-oriented data engineering practices including:

- Modularization
- OOP Design
- Logging
- Advanced Exception Handling
- Automated Testing
- Configuration-Driven Design
- Packaging
- CI/CD
- Data Quality Framework
- Rejected Data Layer
- Pipeline Orchestration

---

## Project Objectives

- Implement Medallion Architecture (Bronze, Silver, Gold)
- Build a Data Quality Validation Framework
- Apply Software Engineering Best Practices
- Implement Logging and Advanced Error Handling
- Create Automated Unit Tests
- Package the project as an installable Python package
- Implement CI/CD using GitHub Actions
- Create an Orchestrated ETL Execution Framework
- Improve Data Quality while minimizing unnecessary data loss

---

## Architecture

```text
Source Data
    |
    v
Bronze Layer
    |
    v
Silver Validation
    |
    +----------------------+
    |                      |
    v                      v
Data Quality Report
    |
    v
Silver Cleaning
    |
    v
Silver Dataset
    |
    +----------------------+
    |                      |
    v                      v
Gold Sales Dataset   Gold Customer Dataset
```

---

## Project Structure

```text
Use_Case_2

├── .github/
│   └── workflows/
│       └── ci.yml
│
├── data/
│   ├── source/
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   ├── rejected/
│   └── reports/
│
├── logs/
│
├── src/
│   ├── config/
│   ├── ingestion/
│   ├── transformation/
│   ├── utils/
│   └── main.py
│
├── tests/
│
├── requirements.txt
├── setup.py
└── README.md
```

---

## Bronze Layer

### Responsibilities

- Read source CSV
- Add metadata columns
- Store raw data without modification
- Write Bronze Layer in Parquet format

### Metadata Columns Added

- ingestion_timestamp
- load_date
- source_file_name

### Metadata Purpose

- ingestion_timestamp – Captures exact ingestion timestamp for auditability.
- load_date – Stores pipeline execution date.
- source_file_name – Maintains source lineage and traceability.

### Output

```text
data/bronze/
```

---

## Silver Validation Layer

Implemented data quality validations before applying any cleaning logic.

### Completeness Checks

- Null Order ID
- Null Customer ID
- Null Customer Name
- Null Product ID
- Null Product Name
- Blank Customer Name

### Uniqueness Checks

- Duplicate Row ID

### Validity Checks

- Invalid Country
- Invalid Ship Mode
- Invalid Customer ID
- Invalid Product ID

### Date Quality Checks

- Future Order Date
- Ship Date Before Order Date
- Invalid Order Date Format
- Invalid Ship Date Format

### Business Rule Checks

- Negative Sales Amount
- Invalid Quantity

### Schema Validation

- Mandatory Column Validation

---

## Data Quality Report

Generated Output:

```text
data/reports/data_quality_report.csv
```

Contains:

- Validation Rule Name
- Error Count

Example:

```text
Null Order ID
Duplicate Row ID
Invalid Country
Invalid Ship Mode
Future Order Date
Negative Sales
```

---

## Silver Cleaning Layer

Instead of blindly deleting all problematic records, business-oriented cleansing strategies were implemented.

### Rejected Records

Records that could not be reliably corrected:

- NULL Order IDs
- Future Order Dates

Stored in:

```text
data/rejected/
```

Examples:

```text
data/rejected/null_order_ids/

data/rejected/future_order_dates/
```

### Standardized Attributes

Descriptive attributes were standardized instead of removing entire records:

```text
Invalid Country
    ↓
UNKNOWN_COUNTRY

Invalid Ship Mode
    ↓
UNKNOWN_SHIP_MODE

NULL Customer Name
    ↓
UNKNOWN_CUSTOMER
```

### Business Review Flags

Records requiring business review are retained:

```text
NULL Product ID
    ↓
MISSING_PRODUCT_ID

Negative Sales
    ↓
REVIEW_REQUIRED
```

### Deduplication

Duplicate records are removed using:

```python
dropDuplicates(["row_id"])
```

### Output

```text
data/silver/
```

---

## Rejected Layer

A dedicated rejected layer was implemented to preserve unrecoverable records rather than deleting them permanently.

### Current Rejected Datasets

```text
data/rejected/

    null_order_ids/

    future_order_dates/
```

## Gold Sales Dataset

### Purpose

Business-ready sales reporting dataset.

### Attributes

- order_id
- customer_id
- order_date
- ship_date
- ship_mode
- city

### Output

```text
data/gold/sales_gold/
```

### Common Key

```text
customer_id
```

acts as the shared key between Sales Gold and Customer Gold datasets.

---

## Gold Customer Dataset

### Purpose

Customer Analytics and KPI Generation.

### Attributes

- customer_id
- first_name
- last_name
- segment
- country

### Customer KPIs

- Orders Last 30 Days
- Orders Last 6 Months
- Orders Last 12 Months
- Orders All Time

### Output

```text
data/gold/customer_gold/
```

### Common Key

```text
customer_id
```

acts as the shared key between Sales Gold and Customer Gold datasets.

---

## Logging

Centralized logging framework implemented using Python Logging.

### Features

- File Logging
- Console Logging
- YAML-Driven Log Level Configuration

### Log Levels

- INFO
- WARNING
- ERROR

### Output Location

```text
logs/
```

---

## Advanced Exception Handling

Implemented a custom ETL exception framework.

### Captured Information

- Timestamp
- ETL Layer
- File Name
- Function Name
- Line Number
- Error Type
- Root Cause

### Benefits

- Faster Debugging
- Better Production Support
- Standardized Error Reporting

---

## Configuration Driven Design

Project behavior is externalized using:

```text
src/config/config.yaml
```

### Configuration Includes

- Data Paths
- Validation Rules
- Logging Configuration
- Spark Configuration
- Data Quality Thresholds
- Rejected Layer Configuration

### Benefits

- Reduced Hardcoding
- Improved Maintainability
- Easier Environment Migration

---

## Automated Testing

Implemented using PyTest.

### Utility Layer Tests

- Config Reader
- Logger
- Exception Framework
- Spark Session

### Bronze Layer Tests

- Read Source
- Metadata Validation
- Write Validation
- Wrong Path Validation

### Silver Validation Tests

- Data Quality Validation
- Country Validation
- Ship Mode Validation
- Null Validation

### Silver Cleaning Tests

- Record Rejection
- Data Standardization
- Review Flag Generation
- Deduplication Logic
- Rejected Layer Validation

### Gold Layer Tests

- Sales Gold Dataset Validation
- Customer Gold Dataset Validation
- Customer KPI Validation

### Test Execution

```bash
python -m pytest tests -v
```

---

## Packaging

Project is installable as a Python package.

### Installation

```bash
pip install .
```

---

## CI/CD

Implemented using GitHub Actions.

### Workflow File

```text
.github/workflows/ci.yml
```

### Automated Steps

- Install Dependencies
- Package Validation
- Execute ETL Pipeline
- Execute Automated Tests

---

## Orchestration

Pipeline orchestration implemented using:

```text
src/main.py
```

### Execute Complete Pipeline

```bash
python -m src.main
```

### Execution Flow

```text
Bronze Layer
    ↓
Silver Validation
    ↓
Silver Cleaning
    ↓
Sales Gold
    ↓
Customer Gold
```

---

## Software Engineering Best Practices Implemented

- OOP Design
- Modular Architecture
- Reusable Components
- Configuration-Driven Design
- Logging Framework
- Advanced Exception Handling
- Automated Testing
- Packaging
- CI/CD
- Orchestration
- Rejected Data Layer

---

## Architecture DAG

See:

```text
docs/architecture_dag.txt
```

---

## Spark Optimizations

### Coalesce

Gold datasets use:

```python
coalesce(1)
```

to produce business-friendly outputs.

### Pipeline Segregation

Pipeline separated into:

- Bronze Layer
- Silver Validation
- Silver Cleaning
- Gold Sales
- Gold Customer

to improve maintainability and fault isolation.

### Reusable Spark Session

Implemented a centralized:

```python
SparkSessionManager
```

to avoid duplicate Spark initialization.

### Configuration Driven Design

All configurable values are externalized through:

```yaml
config.yaml
```

reducing hardcoded business logic.

---

## Enhancements Beyond Requirements

- Data Quality Framework
- Rejected Data Layer
- Advanced Exception Framework
- YAML-Based Configuration Management
- GitHub Actions CI/CD
- Packaging using setuptools
- End-to-End Orchestration
- 40+ Automated Tests
- Business-Oriented Data Cleansing Strategy
- Customer KPI Generation
- Common Gold Layer Key Design

---

## Future Enhancements

- Data Quality Dashboard
- Environment-Based Configuration
- Incremental Processing
- Monitoring & Alerting Framework
- Cloud Deployment
- Advanced Data Profiling
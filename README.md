# Sales ETL Pipeline

## Overview

This project implements an end-to-end Data Engineering pipeline using PySpark and follows the Medallion Architecture pattern.

### Pipeline Flow

```text
Source CSV
    ↓
Bronze Layer
    ↓
Silver Validation
    ↓
Silver Cleaning
    ↓
Silver Layer
    ↓
Gold Sales
    ↓
Gold Customer
```

The solution applies enterprise-grade software engineering and data engineering principles including:

- OOP Design
- Modular Architecture
- Centralized Transformation Layer
- Logging Framework
- Advanced Exception Handling
- Automated Testing
- Configuration-Driven Design
- Packaging
- CI/CD
- Data Quality Framework
- Rejected Data Layer
- Pipeline Orchestration
- Partitioned Data Lake Design

---

## Project Objectives

- Implement Medallion Architecture (Bronze, Silver, Gold)
- Build a Data Quality Validation Framework
- Apply Software Engineering Best Practices
- Implement Logging and Advanced Error Handling
- Create Automated Unit Tests
- Package the Project as an Installable Python Package
- Implement CI/CD using GitHub Actions
- Create an Orchestrated ETL Framework
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
Silver Layer
    |
    +----------------------+
    |                      |
    v                      v
Gold Sales Dataset   Gold Customer Dataset
```

---

## Project Structure

```text
Sales_ETL_Project

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
│
│   ├── config/
│   │   └── config.yaml
│   │
│   ├── ingestion/
│   │   └── bronze_loader.py
│   │
│   ├── sales_data_pipeline/
│   │   ├── pipeline.py
│   │   └── transformations.py
│   │
│   ├── utils/
│   │   ├── config_reader.py
│   │   ├── exception.py
│   │   ├── logger.py
│   │   └── spark_session.py
│   │
│   └── main.py
│
├── tests/
│   ├── test_ingestion.py
│   ├── test_pipeline.py
│   ├── test_transformations.py
│   └── test_utils.py
│
├── pyproject.toml
├── requirements.txt
├── setup.py
└── README.md
```

---

## Centralized Transformation Layer

To improve maintainability and readability, all business transformations are centralized into:

```text
src/sales_data_pipeline/transformations.py
```

### Responsibilities

- Silver Validation
- Silver Cleaning
- Sales Gold Processing
- Customer Gold Processing
- Data Quality Reporting
- Rejected Data Management

### Benefits

- Reduced File Proliferation
- Easier Navigation
- Centralized Business Logic
- Simplified Unit Testing
- Improved Maintainability

---

## Bronze Layer

### Responsibilities

- Read Source CSV
- Add Metadata Columns
- Store Raw Data Without Modification
- Partition Data by Year
- Write Bronze Layer in Parquet Format

### Metadata Columns Added

- ingestion_timestamp
- load_date
- source_file_name
- order_year

### Metadata Purpose

| Column | Purpose |
|----------|----------|
| ingestion_timestamp | Captures exact ingestion timestamp |
| load_date | Stores pipeline execution date |
| source_file_name | Maintains source lineage |
| order_year | Enables partition pruning and faster querying |

### Output

```text
data/bronze/
```

---

## Silver Validation Layer

Implemented before applying any cleaning logic.

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

Examples:

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

Business-oriented cleansing strategies were applied.

### Rejected Records

Unrecoverable records are moved to the Rejected Layer.

Examples:

```text
NULL Order IDs
Future Order Dates
```

Output:

```text
data/rejected/
```

Examples:

```text
data/rejected/null_order_ids

data/rejected/future_order_dates
```

### Standardized Attributes

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

```text
NULL Product ID
↓
MISSING_PRODUCT_ID

Negative Sales
↓
REVIEW_REQUIRED
```

### Deduplication

```python
dropDuplicates(["row_id"])
```

### Output

```text
data/silver/
```

---

## Partitioning Strategy

### Bronze Layer

Partitioned By:

```text
order_year
```

Output:

```text
data/bronze/
├── order_year=2023
├── order_year=2024
├── order_year=2025
└── order_year=2099
```

### Silver Layer

Partitioned By:

```text
order_year
```

Output:

```text
data/silver/
├── order_year=2023
├── order_year=2024
└── order_year=2025
```

Future-dated records are removed before writing Silver.

### Sales Gold Layer

Partitioned By:

```text
order_year
```

Output:

```text
data/gold/sales/
├── order_year=2023
├── order_year=2024
└── order_year=2025
```

### Benefits

- Faster Queries
- Partition Pruning
- Better Scalability
- Production-Oriented Data Lake Design

---

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
- order_year

### Output

```text
data/gold/sales/
```

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
data/gold/customer/
```

---

## Logging Framework

Centralized logging implemented using Python Logging.

### Features

- File Logging
- Console Logging
- Configurable Log Levels
- Centralized Log Management

### Log Levels

- INFO
- WARNING
- ERROR

### Output

```text
logs/
```

---

## Advanced Exception Handling

Implemented using:

```python
PipelineException
```

and

```python
_handle_exception()
```

### Captured Information

- Timestamp
- ETL Layer
- Error Type
- File Name
- Function Name
- Line Number
- Root Cause

### Benefits

- Centralized Exception Handling
- Faster Debugging
- Improved Production Support
- Standardized Error Reporting

---

## Configuration Driven Design

Configuration stored in:

```text
src/config/config.yaml
```

### Contains

- Data Paths
- Validation Rules
- Logging Configuration
- Spark Configuration
- Data Quality Thresholds
- Rejected Layer Settings

### Benefits

- Reduced Hardcoding
- Easier Maintenance
- Environment Portability

---

## Automated Testing

Implemented using PyTest.

### Test Coverage

#### test_ingestion.py

- Source Read Validation
- Metadata Validation
- Bronze Write Validation
- Wrong File Path Validation

#### test_pipeline.py

- Extract Layer
- Validate Layer
- Clean Layer
- Transform Layer
- Load Layer
- End-to-End Run Validation

#### test_transformations.py

- Validation Rules
- Cleaning Rules
- Silver Processing
- Sales Gold Processing
- Customer Gold Processing
- Report Generation

#### test_utils.py

- Config Reader
- Logger
- Exception Framework
- Spark Session

### Test Execution

```bash
python -m pytest tests -v
```

### Current Coverage

```text
37 Passing Tests
```

---

## Packaging

Installable Python Package.

### Installation

```bash
pip install .
```

or

```bash
pip install -e .
```

---

## CI/CD

Implemented using GitHub Actions.

### Workflow

```text
.github/workflows/ci.yml
```

### Automated Steps

- Install Dependencies
- Package Validation
- Execute Unit Tests
- Execute ETL Pipeline

---

## Orchestration

Pipeline orchestration implemented via:

```text
src/main.py
```

### Execute Full Pipeline

```bash
python -m src.main
```

### Execution Flow

```text
Bronze
 ↓
Silver Validation
 ↓
Silver Cleaning
 ↓
Gold Sales
 ↓
Gold Customer
```

---

## Software Engineering Best Practices Implemented

- OOP Design
- Modular Architecture
- Centralized Transformation Layer
- Reusable Components
- Configuration Driven Design
- Logging Framework
- Advanced Exception Handling
- Automated Testing
- Packaging
- CI/CD
- Orchestration
- Rejected Data Layer
- Partitioned Data Lake Design

---

## Enhancements Beyond Requirements

- Data Quality Framework
- Rejected Data Layer
- Centralized Transformation Layer
- Advanced Exception Framework
- YAML Configuration Management
- Partitioned Data Lake Architecture
- Python Package Distribution
- GitHub Actions CI/CD
- 37 Automated Unit Tests
- Business-Oriented Data Cleansing Framework
- Customer KPI Generation
- Data Quality Reporting

---

## Future Enhancements

- Apache Airflow Orchestration
- Azure Data Factory Integration
- Delta Lake Implementation
- Incremental Processing
- Data Quality Dashboard
- Monitoring & Alerting Framework
- Environment-Based Configuration
- ADLS / S3 Integration
- Great Expectations Integration
- Automated Data Profiling
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

The project was built using Software Engineering best practices including:

- Modularization
- OOP Design
- Logging
- Exception Handling
- Unit Testing
- Packaging
- CI/CD
- Data Quality Framework
- Orchestration

---

## Project Objectives

- Implement Medallion Architecture (Bronze, Silver, Gold)
- Build Data Quality Validation Framework
- Apply Software Engineering Best Practices
- Implement Logging and Error Handling
- Create Automated Unit Tests
- Package the project as an installable Python package
- Implement CI/CD using GitHub Actions
- Create an orchestrated ETL execution flow

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
- Write Bronze Parquet dataset

### Metadata Columns Added

- ingestion_timestamp
- load_date
- source_file_name

### Output

```text
data/bronze/
```

---

## Silver Validation Layer

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

- Required Column Validation

---

## Data Quality Report

Generated Output:

```text
data/reports/data_quality_report.csv
```

Contains:

- Validation Rule Name
- Error Count

---

## Silver Cleaning Layer

### Removed Records

- Null Order ID
- Null Product ID
- Null Customer Name
- Invalid Country
- Invalid Ship Mode
- Negative Sales Amount
- Duplicate Row ID

### Clean Silver Dataset

```text
data/silver/
```

---

## Gold Sales Dataset

### Attributes

- Order ID
- Order Date
- Ship Date
- Ship Mode
- City

### Output

```text
data/gold/sales_gold/
```

---

## Gold Customer Dataset

### Attributes

- Customer ID
- First Name
- Last Name
- Segment
- Country

### Metrics

- Orders Last 30 Days
- Orders Last 6 Months
- Orders Last 12 Months
- Orders All Time

### Output

```text
data/gold/customer_gold/
```

---

## Logging

Centralized logging implemented using Python Logging.

### Log Levels

- INFO
- WARNING
- ERROR

### Log Location

```text
logs/
```

---

## Error Handling

Implemented using custom exception handling:

```python
PipelineException
```

Used throughout:

- Bronze Layer
- Silver Validation
- Silver Cleaning
- Gold Layer

---

## Unit Testing

Implemented using PyTest.

### Bronze Tests

- Read Source Success
- DataFrame Validation
- Wrong File Path
- Metadata Validation
- Bronze Write Validation

### Silver Validator Tests

- Read Bronze
- Expected Columns Validation
- Null Validation
- Invalid Country Validation

### Silver Cleaning Tests

- Null Order ID Removal
- Null Product ID Removal
- Duplicate Removal

### Sales Gold Tests

- Read Silver
- Gold Data Creation
- Column Validation

### Customer Gold Tests

- First Name Validation
- Last Name Validation
- Customer Metrics Validation

### Test Execution

```bash
python -m pytest tests -v
```

---

## Packaging

Project is installable as a Python package.

### Install

```bash
pip install .
```

---

## CI/CD

Implemented using GitHub Actions.

Workflow file:

```text
.github/workflows/ci.yml
```

### Automated Steps

- Install Dependencies
- Package Validation
- Bronze Tests
- Silver Tests
- Gold Tests

---

## Orchestration

Pipeline orchestration implemented using:

```text
src/main.py
```

### Run Complete Pipeline

```bash
python -m src.main
```

Execution Sequence:

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
- Centralized Configuration
- Logging Framework
- Exception Handling
- Automated Testing
- Packaging
- CI/CD
- Orchestration Layer

---

## Architecture DAG

See:

```text
docs/architecture_dag.txt
```

---

## Spark Optimizations

The following Spark optimization techniques were implemented or considered during development:

### Coalesce

Gold datasets use:

```python
coalesce(1)
```

to generate a single output file for easier business consumption.

### Pipeline Segregation

The pipeline is separated into:

- Bronze
- Silver Validation
- Silver Cleaning
- Gold Sales
- Gold Customer

This minimizes maintenance effort and isolates failures.

### Reusable Spark Session

A centralized SparkSessionManager was implemented to avoid duplicate Spark configuration and improve maintainability.

### Configuration Driven Design

All paths are managed through:

```yaml
config.yaml
```

This avoids hardcoded values and improves portability.



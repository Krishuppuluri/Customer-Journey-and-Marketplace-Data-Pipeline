# Senior Data Engineer Project - Enterprise Data Pipeline and Modeling

This repository contains the implementation details for an end-to-end data engineering solution, including data modeling, pipeline architecture, real-time ingestion, and feature engineering, tailored to support machine learning and business analytics workflows.

## Table of Contents

1. [Project Overview](#project-overview)
2. [Features](#features)
3. [Data Model Design](#data-model-design)
4. [Pipeline Architecture](#pipeline-architecture)
5. [Real-Time Data Integration](#real-time-data-integration)
6. [Feature Engineering](#feature-engineering)
7. [Technologies Used](#technologies-used)
8. [How to Use](#how-to-use)
9. [Acknowledgments](#acknowledgments)

---

## Project Overview

The project demonstrates the design and implementation of a robust data pipeline using the Medallion Architecture (Bronze, Silver, and Gold layers). It is designed to handle batch and streaming data while preparing ML-ready features from transactional and user interaction data.

---

## Features

- **Data Modeling**: Comprehensive entity-relationship design for key business entities such as customers, transactions, interactions, events, and sessions.
- **Pipeline Architecture**: Scalable and resilient ETL process using Databricks, Spark Structured Streaming, and Delta Lake.
- **Real-Time Ingestion**: High-quality streaming data processing with schema enforcement, deduplication, and validation.
- **Feature Engineering**: Optimized user-level features for machine learning, including purchase frequency, total spend, and session insights.
- **ML Integration**: Gold-layer datasets optimized for analytics and ML feature stores.

---

## Data Model Design

### Key Entities

1. **Customers**: Central entity capturing user details.
   - Attributes: `customer_id`, `name`, `email`, `registration_date`, `customer_segment`, etc.
2. **Transactions**: Records of customer purchases.
   - Attributes: `transaction_id`, `customer_id`, `amount`, `currency`, `transaction_date`, etc.
3. **Interactions**: Logs of communication between customers and the business.
   - Attributes: `interaction_id`, `customer_id`, `interaction_date`, `interaction_type`, etc.
4. **Events**: Tracks granular customer actions on the platform.
   - Attributes: `event_id`, `customer_id`, `event_type`, `event_timestamp`, etc.
5. **Sessions**: Logs of customer activity within specific sessions.
   - Attributes: `session_id`, `customer_id`, `device_type`, `session_start`, `session_end`, etc.

### Relationships
- One-to-Many relationships between `Customers` and other entities for analytical and ML-ready data.
- Designed for feature extraction, e.g., `average_session_duration`, `transaction_frequency_last_30_days`.

---

## Pipeline Architecture

### Overview

The pipeline is implemented using the Medallion Architecture:

1. **Bronze Layer**: Raw data ingestion from batch and streaming sources with schema enforcement.
2. **Silver Layer**: Cleaned and validated data with deduplication, null handling, and enrichment.
3. **Gold Layer**: Optimized datasets with engineered features for machine learning and analytics.

### Key Features

- **Batch and Streaming Ingestion**: Handles CSV, JSON, and real-time streams using Spark.
- **Delta Lake**: Provides fault tolerance and real-time processing capabilities.
- **Optimization**: Z-ordering, partitioning, and Delta OPTIMIZE for fast queries.

---

## Real-Time Data Integration

### Implementation
- **Ingestion**: Spark Structured Streaming simulates transactional data streams.
- **Validation**:
  - Schema enforcement.
  - Null value handling.
  - Deduplication based on unique transaction IDs.
- **Logging**: Tracks pipeline metrics such as total records processed and duplicates removed.

---

## Feature Engineering

### Features Created

- **Purchase Frequency**: Number of unique transactions per user.
- **Total Spend**: Sum of transaction amounts.
- **Average Transaction Value**: Average of transaction amounts.
- **Recency Days**: Days since the last transaction.

### Optimizations

- **Partitioning**: By `user_id` for efficient querying.
- **Z-Ordering**: On `total_spend` for faster retrieval.
- **Delta Optimization**: Compacting small files for storage efficiency.

---

## Technologies Used

- **Databricks**: End-to-end pipeline development.
- **Spark Structured Streaming**: Real-time ingestion.
- **Delta Lake**: Data lake with ACID transactions.
- **Python**: Scripting and pipeline orchestration.
- **MLflow**: Machine learning experimentation and deployment.

---

## How to Use

1. Clone this repository.
2. Set up a Databricks workspace.
3. Configure the pipeline scripts for your data sources (batch or streaming).
4. Run the ingestion, transformation, and feature engineering pipelines sequentially.
5. Use Gold-layer datasets for analytics or ML models.

---

## Acknowledgments

Thank you for the opportunity to work on this challenging and insightful project. It provided a platform to demonstrate real-world data engineering and machine learning preparation skills.

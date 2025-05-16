# Airline Loyalty Unstructured Data Processing Pipeline

## Project Overview

A robust, scalable Spark-based data engineering pipeline designed to process unstructured airline loyalty program transaction data. This pipeline ingests raw text and JSON files from various sources, normalizes them to a common schema, and outputs structured, analysis-ready data to AWS S3.

## Use Case
Airlines generate massive amounts of loyalty program transaction data across multiple channels and in various formats. This data contains valuable insights about customer behavior, preferences, and engagement patterns. Our pipeline:

    - Extracts structured information from unstructured text logs and JSON files
    - Standardizes data across disparate formats
    - Enables real-time processing of loyalty transactions
    - Supports downstream analytics for personalization, targeted marketing, and program optimization

##  Challenges in Legacy Systems
The legacy data processing approach suffered from several limitations:

    - **Format Inconsistency**: Transaction data arrived in multiple formats (free text, semi-structured logs, JSON) with no standard schema
    - **Error Handling**: Poor error isolation led to entire batch failures when processing problematic records
    - **Scalability Issues**: Fixed resource allocation couldn't handle variable load patterns
    - **Limited Monitoring**: Difficult to track processing success/failure and data quality


##  Solution Architecture
Our solution implements a fault-tolerant, efficient Spark streaming pipeline:

    - **Data Ingestion**: Monitors S3 buckets for new text and JSON loyalty transaction files
    - **Parsing Layer**: Applies optimized extraction logic for both formats
    - **Transformation**: Normalizes to a common schema for unified downstream processing
    - **Output**: Writes structured Parquet files to S3 for analytics workloads
    - **Monitoring**: Real-time status tracking and error reporting


##  Directory Structure
```
airline-loyalty-pipeline/
│
├── docker-compose.yml          # Docker configuration for Spark cluster
├── Dockerfile                  # Spark container configuration
├── submit-spark-job.sh         # Convenience script for job submission
│
├── src/                        # Source code
│   ├── main.py                 # Main Spark streaming application
│   ├── config/                 # Configuration management
│   │   └── config.py           # Application configuration
│   │
│   └── parsers/                # Data parsing modules
│       ├── __init__.py
│       ├── text_parser.py      # Functions for extracting data from text logs
│       └── json_parser.py      # Functions for processing JSON data
│
└── .env                        # Environment variables (AWS credentials, etc.)

```


##  Technologies Used

- Apache Spark: Distributed data processing framework
- Python 3: Core programming language
- Docker: Container runtime for deployment
- Bitnami Spark Image: Base image for Spark containers
- AWS S3: Storage for input and output data
- AWS IAM: Access management for S3


##  How It Works

####  1. Data Organization: Raw data files should be uploaded to:

- Text files: s3://your-bucket-name/raw/text/
- JSON files: s3://your-bucket-name/raw/json/


####  2. Processing Flow:

- Spark streaming monitors these locations for new files
- Text files are split into individual transactions and parsed
- JSON files are processed according to a predefined schema
- Both streams are unified to a common schema
- Processed data is written to s3://your-bucket-name/processed/


####  3. Data Schema:
The final processed data contains the following fields:

- customer_id: Loyalty program member ID
- flight_no: Flight identifier
- departure: Departure airport code
- arrival: Arrival airport code
- transaction_date: Date of transaction
- earned_points: Loyalty points earned
- transaction_ref: Unique transaction reference
- channel: Transaction channel (mobile, web, kiosk, etc.)
- additional_data: JSON string containing extra transaction details
- source_type: Data source type (text or JSON)
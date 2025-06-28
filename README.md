# Clickstream Analytics Project

## Project Overview
Real-time clickstream analytics pipeline using Google Cloud Platform (GCP) services to process both batch and streaming data, with visualizations in Looker Studio.

## Architecture

### Streaming Pipeline
- **Data Generator** → Pub/Sub topic 'clicks'
- **Dataflow** job (us-central1)
- **BigQuery** table: `clickstream_analytics.clicks_raw`

### Batch Pipeline
- **Source**: GCS `gs://click-stream-data-sp/batch_data/clothing.csv`
- **Dataflow** processing
- **BigQuery** table: `clickstream_analytics.clickstream_batch`

## Components

### 1. Data Generator (`data_generator.py`)
- Generates realistic clickstream events
- Fields: timestamp, user_id, session_id, url, event_type, etc.
- Publishes to Pub/Sub topic 'clicks'

### 2. Batch Pipeline (`batch_pipeline.py`)
- Processes historical data from GCS
- 165,474 records processed
- Uses Apache Beam for data transformation

### 3. Looker Studio Dashboard
- **Key Metrics**:
  - Unique Users
  - Total Sessions
  - Total Purchases
- **Visualizations**:
  - Time series of events
  - Category distribution
  - Event type breakdown

## Setup Instructions

1. **Environment Setup**
```bash
pip install apache-beam[gcp] google-cloud-pubsub faker
```

2. **Authentication**
```bash
gcloud auth application-default login
```

3. **Run Data Generator**
```bash
python data_generator.py
```

4. **Run Batch Pipeline**
```bash
python batch_pipeline.py
```

## Project Structure
```
ClickStream_Project/
├── data_generator.py      # Streaming data generator
├── batch_pipeline.py      # Batch data processor
├── streaming_insights.sql # SQL queries for analytics
├── streaming_schema.json  # BigQuery schema definition
└── README.md             # Project documentation
```

## Security Considerations
- GCP service account authentication
- BigQuery table access controls
- Sensitive data handling in schema

## Monitoring
- Dataflow job status in GCP Console
- BigQuery data freshness
- Pub/Sub message flow

## Project Details
- Project ID: click-stream-463520
- Region: us-central1
- BigQuery Dataset: clickstream_analytics

## Overview
This project implements a comprehensive clickstream analytics solution using Google Cloud Platform (GCP) services. It processes both batch and streaming clickstream data, derives insights, and visualizes them in Looker Studio.

## Components

### 1. Batch Processing Pipeline
- Processes historical CSV data from Cloud Storage
- Uses Apache Beam with Dataflow runner
- Loads data into BigQuery table: `clickstream_analytics.clickstream_batch`

### 2. Streaming Pipeline
- Processes real-time clickstream events
- Uses Pub/Sub → Dataflow → BigQuery
- Destination table: `clickstream_analytics.clicks_raw`
- Includes dead-letter table for error handling

### 3. Data Generator
- Simulates real-time clickstream events
- Generates realistic user behavior data
- Publishes to Pub/Sub topic: `clicks`

### 4. Analytics
- SQL queries for both batch and streaming data
- Key metrics include:
  - User engagement patterns
  - Product performance
  - Error analysis
  - Response time monitoring

### 5. Visualization
- Interactive dashboards in Looker Studio
- Real-time and historical data views
- Complete dashboard documentation available in [docs/dashboard.pdf](docs/dashboard.pdf)

## Setup Instructions

1. **Environment Setup**
   ```bash
   # Install required packages
   pip install google-cloud-pubsub
   pip install faker
   pip install apache-beam[gcp]
   ```

2. **Configuration**
   - Project ID: click-stream-463520
   - Region: us-central1
   - Dataset: clickstream_analytics

3. **Running the Pipelines**
   - Batch Pipeline: `python batch_pipeline.py`
   - Data Generator: `python data_generator.py`
   - Streaming Pipeline: Use Dataflow template

## Security Measures
- IAM roles for access control
- Data masking for sensitive fields
- Audit logging enabled

## Directory Structure
```
clickstream-analytics/
├── data_generator.py          # Streaming data generator
├── streaming_insights.sql     # SQL queries for analytics
├── streaming_schema.json      # BigQuery table schema
└── README.md                 # Project documentation
```

## Monitoring
- Dataflow job metrics
- BigQuery query performance
- Pub/Sub message flow

## Contact
For questions or issues, please contact [Your Contact Information]

# Clickstream Analytics Project Documentation

## Architecture Overview
- Data Generator → Pub/Sub → Dataflow → BigQuery
- Batch Processing: GCS → Dataflow → BigQuery
- Analytics: BigQuery → Looker Studio

## Components

### 1. Data Generator
- Simulates real-time user activity
- Publishes to Pub/Sub topic: 'clicks'
- Configurable event types and frequencies

### 2. Data Processing
- **Batch Pipeline**: Processes historical data
- **Streaming Pipeline**: Real-time event processing
- **Error Handling**: Dead-letter table for invalid records

### 3. Data Storage
- **Raw Events**: `clickstream_analytics.clicks_raw`
- **Batch Data**: `clickstream_analytics.clickstream_batch`
- **Schema**: See `streaming_schema.json`

### 4. Analytics
- **Batch Insights**: `batch_insights.sql`
- **Streaming Insights**: `insights.sql`
- **Dashboard**: See `docs/dashboard.pdf`

## Monitoring
- Dataflow job metrics
- BigQuery query performance
- Pub/Sub message flow

## Troubleshooting
1. **Data Generator Issues**
   - Verify GCP credentials
   - Check Pub/Sub permissions

2. **Pipeline Issues**
   - Monitor Dataflow jobs
   - Check error records in dead-letter table

3. **Dashboard Issues**
   - Verify BigQuery permissions
   - Check query performance

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import logging

class TransformDataFn(beam.DoFn):
    def process(self, element):
        fields = element.split(',')
        try:
            yield {
                'timestamp': fields[0],
                'user_id': int(fields[1].split('user_')[1]),
                'session_id': int(fields[2].split('session_')[1]),
                'url': fields[3],
                'http_status_code': int(fields[4]),
                'response_time_ms': int(fields[5]),
                'event_type': fields[6],
                'product_id': int(fields[7].split('prod_')[1]),
                'price': float(fields[8]),
                'colour': fields[9],
                'category': fields[10],
                'geolocation': fields[11],
                'user_agent': fields[12],
                'entry_url': fields[13],
                'exit_url': fields[14],
                'session_start_time': fields[15],
                'session_end_time': fields[16],
                'click_timestamp': fields[17],
                'error_type': fields[18],
                'error_message': fields[19],
                'first_visit_timestamp': fields[20],
                'session_count': int(fields[21]),
                'country': fields[11].split(', ')[1] if ', ' in fields[11] else None
            }
        except Exception as e:
            logging.error(f"Error transforming record: {element}, {e}")

def run():
    options = PipelineOptions([
        '--project=click-stream-463520',
        '--job_name=batch-clickstream-to-bigquery',
        '--temp_location=gs://click-stream-data-sp/temp',
        '--region=us-central1',
        '--runner=DataflowRunner'
    ])
    with beam.Pipeline(options=options) as pipeline:
        (
            pipeline
            | 'Read CSV' >> beam.io.ReadFromText('gs://click-stream-data-sp/batch_data/clothing.csv', skip_header_lines=1)
            | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
                'click-stream-463520:clickstream_analytics.clickstream_batch',
                schema='SCHEMA_AUTODETECT',
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
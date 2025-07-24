import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery
import datetime
import csv
import logging
from google.cloud import storage

class ParseDepartmentFn(beam.DoFn):
    def __init__(self, raw_file_size):
        self.raw_file_size = raw_file_size

    def process(self, element):
        try:
            if not element.strip():
                return []
            row = next(csv.reader([element]))
            parsed = {
                'DepartmentID': int(row[0]),
                'Name': row[1],
                'GroupName': row[2],
                'ModifiedDate': row[3],
                'RawIngestionTime': datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
                'RawFileSize': self.raw_file_size
            }
            logging.info(f"Parsed row: {parsed}")
            return [parsed]
        except Exception as e:
            logging.warning(f"Failed to parse: {element} | Error: {e}")
            return []

def get_file_size(bucket_name, blob_name):
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blob = bucket.get_blob(blob_name)
    size_in_kb = blob.size / 1024
    return f"{size_in_kb:.2f} KB"

def run():
    bucket_name = 'gcp-de-batch-data-3'
    blob_name = 'Department.csv'
    file_size = get_file_size(bucket_name, blob_name)

    print(f"Fetched file size from GCS: {file_size}")

    options = PipelineOptions(
        runner='DataflowRunner',
        project='gcp-de-batch-sim-464816',
        temp_location='gs://gcp-de-batch-data-3/temp',
        staging_location='gs://gcp-de-batch-data-3/staging',
        region='us-east1',
        job_name='dep-raw-job',
        save_main_session=True
    )

    input_file = f'gs://{bucket_name}/{blob_name}'
    output_table = 'gcp-de-batch-sim-464816:Employee_Details_raw.Department_raw'

    with beam.Pipeline(options=options) as p:
        (
            p
            | 'Read CSV' >> beam.io.ReadFromText(input_file, skip_header_lines=1)
            | 'Parse Rows with Metadata' >> beam.ParDo(ParseDepartmentFn(file_size))
            | 'Write to BigQuery' >> WriteToBigQuery(
                table=output_table,
                method='STREAMING_INSERTS',
                schema=(
                    'DepartmentID:INTEGER, Name:STRING, GroupName:STRING, '
                    'ModifiedDate:STRING, RawIngestionTime:TIMESTAMP, RawFileSize:STRING'
                ),
                create_disposition='CREATE_IF_NEEDED',
                write_disposition='WRITE_APPEND'
            )
        )

if __name__ == '__main__':
    run()

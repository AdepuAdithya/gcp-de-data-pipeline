import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import json
import yaml
import datetime
from apache_beam.io.gcp.gcsfilesystem import GCSFileSystem

# Load Configurations from Files
def load_config(file_path):
    """Loads a JSON or YAML config file from disk."""
    with open(file_path, 'r') as file:
        if file_path.endswith('.json'):
            return json.load(file)
        elif file_path.endswith('.yaml'):
            return yaml.safe_load(file)

# Load BigQuery Table Configuration
bq_config = load_config("config/bigquery_table_config.json")
bigquery_table = f"{bq_config['tableReference']['projectId']}:{bq_config['tableReference']['datasetId']}.{bq_config['tableReference']['tableId']}"

# Load Dataflow Job Configuration
job_config = load_config("config/dataflow_job_config.yaml")

# Load Schema Definition
bq_schema = load_config("schema/department_raw_schema.json")

class ParseCSV(beam.DoFn):
    """Parses CSV records and adds metadata fields."""
    def process(self, element, timestamp, file_size):
        row = element.split(',')
        yield {
            'DepartmentID': str(row[0]),
            'Name': str(row[1]),
            'GroupName': str(row[2]),
            'ModifiedDate': str(row[3]),
            'RawIngestionTime': str(timestamp),  # Capture ingestion timestamp
            'RawFileSize': int(file_size)  # Capture file size
        }

def get_file_size(gcs_path, pipeline_options):
    """Gets the file size of the CSV from GCS."""
    gcs = GCSFileSystem(pipeline_options)
    matched_files = gcs.match([gcs_path])

    if matched_files and matched_files[0].metadata_list:
        return matched_files[0].metadata_list[0].size_in_bytes
    return None

def run():
    # Define pipeline options
    pipeline_options = PipelineOptions(
        runner="DataflowRunner",
        project=job_config["project"],
        region=job_config["region"],
        temp_location=job_config["temp_location"],
        staging_location=job_config["staging_location"]
    )

    gcs_file_path = job_config["gcs_input_path"]

    # Capture ingestion time
    ingestion_time = str(datetime.datetime.now())

    # Get file size from GCS
    file_size = get_file_size(gcs_file_path, pipeline_options)

    with beam.Pipeline(options=pipeline_options) as p:
        raw_data = (
            p
            | "Read from GCS" >> beam.io.ReadFromText(gcs_file_path, skip_header_lines=1)
            | "CSV Parsing" >> beam.ParDo(ParseCSV(), timestamp=ingestion_time, file_size=file_size)
            | "Write to BigQuery" >> beam.io.WriteToBigQuery(
                table=bigquery_table,
                schema=bq_schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )
        )

if __name__ == '__main__':
    run()
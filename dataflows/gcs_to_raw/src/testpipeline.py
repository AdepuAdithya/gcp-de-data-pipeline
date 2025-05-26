import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import datetime
import sys
import logging

sys.path.append("c:/Users/adith/Documents/GitHub/gcp-de-data-pipeline/dataflows/gcs_to_raw/src/")
from utils import load_schema, load_config, ParseCSV, get_file_size

# Configure logging
logging.basicConfig(level=logging.INFO)

# Load BigQuery Table Configuration
bq_config = load_config("C:/Users/adith/Documents/GitHub/gcp-de-data-pipeline/dataflows/gcs_to_raw/config/bigquery_table_config.json")
bigquery_table = f"{bq_config['tableReference']['projectId']}:{bq_config['tableReference']['datasetId']}.{bq_config['tableReference']['tableId']}"

# Load Dataflow Job Configuration
config = load_config("C:/Users/adith/Documents/GitHub/gcp-de-data-pipeline/dataflows/gcs_to_raw/config/dataflow_job_config.yaml")

# Load Schema Definition
schema = load_schema("C:/Users/adith/Documents/GitHub/gcp-de-data-pipeline/dataflows/gcs_to_raw/schema/department_raw_schema.json")

# Pipeline
def run():
    # Define pipeline options
    pipeline_options = PipelineOptions(
        runner=config["runner"],
        project=bq_config["tableReference"]["projectId"],
        region=config["region"],
        temp_location=config["temp_location"],
        staging_location=config["staging_location"],
        setup_file="C:/Users/adith/Documents/GitHub/gcp-de-data-pipeline/dataflows/gcs_to_raw/setup.py"
    )

    gcs_file_path = config["gcs_input_path"]

    # Capture ingestion time
    ingestion_time = str(datetime.datetime.now())

    # Get file size from GCS
    file_size = get_file_size(gcs_file_path, pipeline_options)

    with beam.Pipeline(options=pipeline_options) as p:
        raw_data = (
            p
            | "Read from GCS" >> beam.io.ReadFromText(gcs_file_path, skip_header_lines=1)
            | "Buffer Rows for Parsing" >> beam.combiners.ToList()  # Efficient grouping alternative

            | "CSV Parsing" >> beam.FlatMap(lambda batch: safe_parse(batch, ingestion_time, file_size))
            | "Write to BigQuery" >> beam.io.WriteToBigQuery(
                table=bigquery_table,
                schema=schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )
        )

# Improved CSV parsing function with error handling
def safe_parse(batch, ingestion_time, file_size):
    for row in batch:
        try:
            parsed_row = ParseCSV(row, timestamp=ingestion_time, file_size=file_size)
            yield parsed_row
        except Exception as e:
            logging.error(f"Error parsing row: {row}, Exception: {e}")

if __name__ == '__main__':
    run()
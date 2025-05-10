import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import datetime
import sys
sys.path.append("c:/Users/adith/Documents/GitHub/gcp-de-data-pipeline/dataflows/gcs_to_raw/src/")
from utils import load_schema, load_config, ParseCSV, get_file_size

# Load BigQuery Table Configuration
bq_config = load_config("C:/Users/adith/Documents/GitHub/gcp-de-data-pipeline/dataflows/gcs_to_raw/config/bigquery_table_config.json")
bigquery_table = f"{bq_config['tableReference']['projectId']}:{bq_config['tableReference']['datasetId']}.{bq_config['tableReference']['tableId']}"

# Load Dataflow Job Configuration
config = load_config("C:/Users/adith/Documents/GitHub/gcp-de-data-pipeline/dataflows/gcs_to_raw/config/dataflow_job_config.yaml")

# Load Schema Definition
schema = load_schema("C:/Users/adith/Documents/GitHub/gcp-de-data-pipeline/dataflows/gcs_to_raw/schema/department_raw_schema.json")

#Pipeline
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
            | "Assign Key for Grouping" >> beam.Map(lambda row: ("key", row))  # Add key for grouping

            | "Wait for All Rows" >> beam.GroupByKey()  # Ensures all rows are read before parsing
            
            | "CSV Parsing" >> beam.FlatMap(lambda batch: [ParseCSV(row, timestamp=ingestion_time, file_size=file_size) for row in batch])
            | "Write to BigQuery" >> beam.io.WriteToBigQuery(
                table=bigquery_table,
                schema=schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )
        )

if __name__ == '__main__':
    run()
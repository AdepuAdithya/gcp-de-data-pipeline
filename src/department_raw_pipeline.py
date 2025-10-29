# Importing Libraries
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions as PO
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from datetime import datetime, date, timezone

# Configurations / Parameters
project = "gcp-batch-sim"
region = "us-central1"
bucket = "gcp-de-batch-data"
file_name = "Department.csv"
dataset = "Employee_Details_raw"
table = "Department_raw"
input = f"gs://{bucket}/{file_name}"
output = f"{project}:{dataset}.{table}"
temp_location = f"gs://{bucket}/temp"
staging_location = f"gs://{bucket}/staging"

# CSV File Parsing
def parsetxt(line):
    fields = line.strip().split(',')
    row = {
        'DepartmentID': fields[0].strip(),
        'Name': fields[1].strip(),
        'GroupName': fields[2].strip(),
        'ModifiedDate': fields[3].strip(),
        'RawIngestionTime': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S'),
        'LoadDate': date.today().isoformat()
    }
    return row

# BigQuery Schema for Raw Layer
schema = {
    'fields': [
        {'name': 'DepartmentID', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'Name', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'GroupName', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'ModifiedDate', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'RawIngestionTime', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'},
        {'name': 'LoadDate', 'type': 'DATE', 'mode': 'REQUIRED'}
    ]
}

# Pipeline Definition
def run():
    options = PO(
        runner='DataflowRunner',
        project=project,
        region=region,
        temp_location=temp_location,
        staging_location=staging_location,
        job_name='dep-raw-job',
        save_main_session=True
    )

    with beam.Pipeline(options=options) as p:
        (
            p
            | 'Read TXT' >> beam.io.ReadFromText(input, skip_header_lines=1)
            | 'Parse TXT' >> beam.Map(parsetxt)
            | 'Write to BigQuery' >> WriteToBigQuery(
                table=output,
                schema=schema,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )
        )

if __name__ == '__main__':
    run()
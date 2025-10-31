# Importing Libraries
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions as PO
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from datetime import datetime, date, timezone

# Configurations / Parameters
project = "famous-athlete-476816-f8"
region = "us-central1"
bucket = "gcp-de-batch-data-01"
file_name = "Employee.csv"
dataset = "Employee_Details_raw"
table = "Employee_raw"
input = f"gs://{bucket}/{file_name}"
output = f"{project}:{dataset}.{table}"
temp_location = f"gs://{bucket}/temp"
staging_location = f"gs://{bucket}/staging"

# CSV File Parsing
def parsetxt(line):
    fields = line.strip().split(',')
    row = {
        'BusinessEntityID': fields[0].strip(),
        'NationalIDNumber': fields[1].strip(),
        'LoginID': fields[2].strip(),
        'DepartmentID': fields[3].strip(),
        'JobTitle': fields[4].strip(),
        'BirthDate': fields[5].strip(),
        'Gender': fields[6].strip(),
        'HireDate': fields[7].strip(),
        'SalariedFlag': fields[8].strip(),
        'VacationHours': fields[9].strip(),
        'SickLeaveHours': fields[10].strip(),
        'CurrentFlag': fields[11].strip(),
        'ModifiedDate': fields[12].strip(),
        'RawIngestionTime': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S'),
        'LoadDate': date.today().isoformat()
    }

    return row

# BigQuery Schema for Raw Layer
schema = {
    'fields': [
            {'name': 'BusinessEntityID', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'NationalIDNumber', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'LoginID', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'DepartmentID', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'JobTitle', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'BirthDate', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'Gender', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'HireDate', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'SalariedFlag', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'VacationHours', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'SickLeaveHours', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'CurrentFlag', 'type': 'STRING', 'mode': 'REQUIRED'},
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
        job_name='emp-raw-job',
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
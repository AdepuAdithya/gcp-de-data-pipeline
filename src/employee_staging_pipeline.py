#Importing Libraries
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions as PO
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from datetime import datetime

#Configurations/Parameters
project ="famous-athlete-476816-f8"
region = "us-central1"
bucket = "gcp-de-batch-data-01"
raw_dataset = "Employee_Details_raw"
staging_dataset = "Employee_Details_stg"
raw_table   = "Employee_raw"
staging_table   = "Employee_stg"
input = f"{project}.{raw_dataset}.{raw_table}"
output = f"{project}.{staging_dataset}.{staging_table}"
temp_location = f"gs://{bucket}/temp"
staging_location = f"gs://{bucket}/staging"

def add_StagingIngestionTime(record):
    record['StagingIngestionTime'] = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
    return record

# BigQuery Schema for Staging Layer
schema = {
    'fields': [
        {'name': 'BusinessEntityID', 'type': 'INTEGER', 'mode': 'REQUIRED'},
        {'name': 'NationalIDNumber', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'LoginID', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'DepartmentID', 'type': 'INTEGER', 'mode': 'REQUIRED'},
        {'name': 'JobTitle', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'BirthDate', 'type': 'DATE', 'mode': 'REQUIRED'},
        {'name': 'Gender', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'HireDate', 'type': 'DATE', 'mode': 'REQUIRED'},
        {'name': 'SalariedFlag', 'type': 'BOOLEAN', 'mode': 'REQUIRED'},
        {'name': 'VacationHours', 'type': 'INTEGER', 'mode': 'REQUIRED'},
        {'name': 'SickLeaveHours', 'type': 'INTEGER', 'mode': 'REQUIRED'},
        {'name': 'CurrentFlag', 'type': 'BOOLEAN', 'mode': 'REQUIRED'},
        {'name': 'ModifiedDate', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'},
        {'name': 'RawIngestionTime', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'},
        {'name': 'LoadDate', 'type': 'DATE', 'mode': 'REQUIRED'},
        {'name': 'StagingIngestionTime', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'}
    ]
}

def parse_date(date_str):
    for fmt in ('%Y-%m-%d', '%m/%d/%Y'):
        try:
            return datetime.strptime(date_str, fmt).date()
        except ValueError:
            continue
    raise ValueError(f"Date {date_str} does not match expected formats")

def convert_types(record):
    # Adjust datetime formats if different in your data
    new_record = {}
    new_record['BusinessEntityID'] = int(record['BusinessEntityID'])
    new_record['NationalIDNumber'] = record['NationalIDNumber']
    new_record['LoginID'] = record['LoginID']
    new_record['DepartmentID'] = int(record['DepartmentID'])
    new_record['JobTitle'] = record['JobTitle']
    new_record['BirthDate'] = parse_date(record['BirthDate'])
    new_record['Gender'] = record['Gender']
    new_record['HireDate'] = parse_date(record['HireDate'])
    new_record['SalariedFlag'] = record['SalariedFlag'].lower() in ['true', '1', 't', 'yes']
    new_record['VacationHours'] = int(record['VacationHours'])
    new_record['SickLeaveHours'] = int(record['SickLeaveHours'])
    new_record['CurrentFlag'] = record['CurrentFlag'].lower() in ['true', '1', 't', 'yes']
    new_record['ModifiedDate'] = datetime.strptime(record['ModifiedDate'], '%d-%m-%Y %H:%M:%S')
    new_record['RawIngestionTime'] = record['RawIngestionTime']
    new_record['LoadDate'] = record['LoadDate']
    return new_record

#Pipeline Configuration
def run():
    options = PO (
        runner='DataflowRunner',
        project=project,
        region=region,
        temp_location=temp_location,
        staging_location=staging_location,
        job_name='emp-staging-job',
        save_main_session=True
    )
    with beam.Pipeline(options=options) as p:
        (
            p
            | 'Read from BigQuery' >> beam.io.ReadFromBigQuery(query=f'SELECT * FROM `{input}`', use_standard_sql=True)
            | 'Convert Field Types' >> beam.Map(convert_types)
            | 'Add Staging Ingestion Time' >> beam.Map(add_StagingIngestionTime)
            | 'Write to BigQuery' >> WriteToBigQuery(
                table=output,
                schema=schema,
                create_disposition = beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition= beam.io.BigQueryDisposition.WRITE_APPEND
            )
        )

if __name__ == '__main__':
    run()
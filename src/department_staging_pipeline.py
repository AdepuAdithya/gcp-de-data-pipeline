import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from datetime import datetime
import argparse

def add_StagingIngestionTime(record):
    record['StagingIngestionTime'] = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
    return record

# BigQuery Schema for Staging Layer
schema = {
    'fields': [
        {'name': 'DepartmentID', 'type': 'INTEGER', 'mode': 'REQUIRED'},
        {'name': 'Name', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'GroupName', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'ModifiedDate', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'},
        {'name': 'RawIngestionTime', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'},
        {'name': 'LoadDate', 'type': 'DATE', 'mode': 'REQUIRED'},
        {'name': 'StagingIngestionTime', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'}
    ]
}

def convert_types(record):
    new_record = {}
    new_record['DepartmentID'] = int(record['DepartmentID'])
    new_record['Name'] = record['Name']
    new_record['GroupName'] = record['GroupName']
    new_record['ModifiedDate'] = datetime.strptime(record['ModifiedDate'], '%d-%m-%Y %H:%M:%S')
    new_record['RawIngestionTime'] = record['RawIngestionTime']
    new_record['LoadDate'] = record['LoadDate']
    return new_record

def run():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input')
    parser.add_argument('--output')
    args, beam_args = parser.parse_known_args()

    options = PipelineOptions(beam_args)
    with beam.Pipeline(options=options) as p:
        (
            p
            | 'Read from BigQuery' >> beam.io.ReadFromBigQuery(query=f'SELECT * FROM `{args.input}`', use_standard_sql=True)
            | 'Convert Field Types' >> beam.Map(convert_types)
            | 'Add Staging Ingestion Time' >> beam.Map(add_StagingIngestionTime)
            | 'Write to BigQuery' >> WriteToBigQuery(
                table=args.output,
                schema=schema,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )
        )

if __name__ == '__main__':
    run()
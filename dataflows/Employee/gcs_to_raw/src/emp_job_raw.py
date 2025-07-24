import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery
import datetime
import csv
import logging

class ParseDepartmentFn(beam.DoFn):
    def __init__(self, raw_file_size):
        self.raw_file_size = raw_file_size

    def process(self, element):
        try:
            if not element.strip():
                return []
            row = next(csv.reader([element]))
            parsed = {
                'BusinessEntityID': row[0],
                'NationalIDNumber': row[1],
                'LoginID': row[2],
                'OrganizationNode': row[3],
                'OrganizationLevel': row[4],
                'JobTitle': row[5],
                'BirthDate': row[6],
                'MaritalStatus': row[7],
                'Gender': row[8],
                'HireDate': row[9],
                'SalariedFlag': row[10],
                'VacationHours': row[11],
                'SickLeaveHours': row[12],
                'CurrentFlag': row[13],
                'RowGuid': row[14],
                'ModifiedDate': row[15],
                'RawIngestionTime': datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
                'RawFileSize': self.raw_file_size
            }
            logging.info(f"Parsed row: {parsed}")
            return [parsed]
        except Exception as e:
            logging.warning(f"Failed to parse: {element} | Error: {e}")
            return []

def run():
    # Hardcoded for now; replace this logic with dynamic GCS size fetch if needed
    file_size = "2483"  # Size in bytes or label it in KB ("2.4KB")

    options = PipelineOptions(
        runner='DataflowRunner',
        project='gcp-de-batch-sim-464816',
        temp_location='gs://gcp-de-batch-data-3/temp',
        staging_location='gs://gcp-de-batch-data-3/staging',
        region='us-east1',
        job_name='emp-raw-job',
        save_main_session=True  # Required for some Beam runners to serialize classes
    )

    input_file = 'gs://gcp-de-batch-data-3/Employee.csv'
    output_table = 'gcp-de-batch-sim-464816:Employee_Details_raw.Employee_raw'

    with beam.Pipeline(options=options) as p:
        (
            p
            | 'Read CSV' >> beam.io.ReadFromText(input_file, skip_header_lines=1)
            | 'Parse Rows with Metadata' >> beam.ParDo(ParseDepartmentFn(file_size))
            | 'Write to BigQuery' >> WriteToBigQuery(
                table=output_table,
                method='STREAMING_INSERTS',
                schema=(
                    'BusinessEntityID:STRING,'
                    'NationalIDNumber:STRING,'
                    'LoginID:STRING,'
                    'OrganizationNode:STRING,'
                    'OrganizationLevel:STRING,'
                    'JobTitle:STRING,'
                    'BirthDate:STRING,'
                    'MaritalStatus:STRING,'
                    'gender:STRING,'
                    'HireDate:STRING,'
                    'SalariedFlag:STRING,'
                    'VacationHours:INTEGER,'
                    'SickLeaveHours:INTEGER,'
                    'CurrentFlag:STRING,'
                    'RowGuid:STRING,'
                    'ModifiedDate:STRING,'
                    'RawIngestionTime:TIMESTAMP,'
                    'RawFileSize:STRING'
                ),
                create_disposition='CREATE_IF_NEEDED',
                write_disposition='WRITE_APPEND'
            )
        )

if __name__ == '__main__':
    run()
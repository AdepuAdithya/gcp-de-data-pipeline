#Importing Libraries
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions as PO
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from datetime import datetime, date

#Configurations/Parameters
project ="gcp-de-batch-sim-464816"
region = "us-central1"
bucket = "gcp-de-batch-data-3"
file_name = "Department.csv"
input = f"gs://{bucket}/{file_name}"
output = f"{project}.df_practice_dataset.df_practice_table"
temp_location = f"gs://{bucket}/temp"
staging_location = f"gs://{bucket}/staging"

# CSV File Parsing
def parsecsv(line):
    fields = line.split(',')
    parsed = {
        'DepartmentID': int(fields[0]),
        'Name': fields[1],
        'GroupName': fields[2],
        'ModifiedDate': fields[3],
        'RawIngestionTime': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'LoadDate': date.today().isoformat()        
    }
    print(f"Parsed output: {parsed}")
    return parsed

#Pipeline Configuration
def run():
    options = PO (
        runner='DataflowRunner',
        project=project,
        temp_location=temp_location,
        staging_location=staging_location,
        job_name='dep-raw-job',
        save_main_session=True
    )
    with beam.Pipeline(options=options) as p:
        (
            p
            | 'Read CSV' >> beam.io.ReadFromText(input, skip_header_lines=1)
            | 'Parse CSV' >> beam.Map(parsecsv)
            | 'Write to BigQuery' >> WriteToBigQuery(
                table=output,
                schema=(
                    'DepartmentID:INTEGER, Name:STRING, GroupName:STRING, '
                    'ModifiedDate:STRING, RawIngestionTime:TIMESTAMP, RawFileSize:STRING, LoadDate:DATE'
                ),
                create_disposition = beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition= beam.io.BigQueryDisposition.WRITE_TRUNCATE
            )
        )

if __name__ == '__main__':
    run()

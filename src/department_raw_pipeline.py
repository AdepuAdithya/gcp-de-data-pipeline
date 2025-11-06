import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from datetime import datetime, date, timezone
import argparse

# CSV File Parsing
def parsetxt(line):
    fields = line.strip().split(',')
    return {
        'DepartmentID': fields[0].strip(),
        'Name': fields[1].strip(),
        'GroupName': fields[2].strip(),
        'ModifiedDate': fields[3].strip(),
        'RawIngestionTime': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S'),
        'LoadDate': date.today().isoformat()
    }

# BigQuery Schema
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

def run():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input')
    parser.add_argument('--output')
    args, beam_args = parser.parse_known_args()

    options = PipelineOptions(beam_args)
    with beam.Pipeline(options=options) as p:
        (
            p
            | 'Read CSV' >> beam.io.ReadFromText(args.input, skip_header_lines=1)
            | 'Parse CSV' >> beam.Map(parsetxt)
            | 'Write to BigQuery' >> WriteToBigQuery(
                table=args.output,
                schema=schema,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )
        )

if __name__ == '__main__':
    run()
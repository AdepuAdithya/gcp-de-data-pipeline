# Load Configurations from Files
import apache_beam as beam
import json
import yaml
from apache_beam.io.gcp.gcsfilesystem import GCSFileSystem 

#BigQuery Configuration Loading
def load_schema(file_path):
    """Loads a JSON config file from disk."""
    with open(file_path, 'r') as file:
        return json.load(file)

#Dataflow Configuration Loading
def load_config(file_path):
    """Loads a YAML config file from disk."""
    with open(file_path, 'r') as file:
        return yaml.safe_load(file)
    
# CSV Parsing
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

#Get File Size
def get_file_size(gcs_path, pipeline_options):
    """Gets the file size of the CSV from GCS."""
    gcs = GCSFileSystem(pipeline_options)
    matched_files = gcs.match([gcs_path])

    if matched_files and matched_files[0].metadata_list:
        return matched_files[0].metadata_list[0].size_in_bytes
    return None
from apache_beam.io.gcp.gcsfilesystem import GCSFileSystem

pipeline_options = PipelineOptions(project="your-project-id")
gcs_fs = GCSFileSystem(pipeline_options)
print(gcs_fs.exists("gs://your-bucket-name/path-to-your-csv-files/sample.csv"))
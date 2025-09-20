import sys
from raw_pipeline import run as run_raw_pipeline
from staging_pipeline import run as run_staging_pipeline

if __name__ == '__main__':
    # Listing all configurations for raw ingestion
    gcs_to_raw_configs = [
        "configs/raw/department_raw_config.json",
        "configs/raw/employee_raw_config.json",]
    # Listing all configurations for staging ingestion
    raw_to_staging_configs = [
        "configs/staging/department_staging_config.json",
        "configs/staging/employee_staging_config.json",]
    

    # Run GCS to Raw Ingestion Pipelines
    for config in gcs_to_raw_configs:
        print(f"Starting Raw Ingestion Pipeline with config: {config}")
        run_raw_pipeline(config)
        print(f"Completed Raw Ingestion Pipeline with config: {config}")
    
    # Run Raw to Staging Ingestion Pipelines
    for config in raw_to_staging_configs:
        print(f"Starting Staging Ingestion Pipeline with config: {config}")
        run_staging_pipeline(config)
        print(f"Completed Staging Ingestion Pipeline with config: {config}")
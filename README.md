# Data Migration Pipelines

This repository contains Python pipeline files for migrating data to Google BigQuery from MongoDB and AWS.

## Description
The repository includes two Python files:
- `mongo_db_pipeline.py`: A pipeline for migrating data from MongoDB to Google BigQuery.
- `mysql_pipeline.py`: A pipeline for migrating data from AWS (via MySQL) to Google BigQuery.

Each Python file represents a separate data migration pipeline and demonstrates an instance of migrating data to BigQuery from different data sources.

## Files
- `mongo_db_pipeline.py`: Contains the pipeline for migrating data from MongoDB to BigQuery.
- `mysql_pipeline.py`: Contains the pipeline for migrating data from AWS (MySQL) to BigQuery.

## Usage
To use the data migration pipelines:
1. Make sure you have access to Google BigQuery and the necessary credentials.
2. Review the configuration settings in each Python file to ensure they match your environment.
3. Run the respective Python file (`mongo_db_pipeline.py` or `mysql_pipeline.py`) using Python 3.x.
4. Follow the prompts and instructions provided in the pipeline script to initiate the data migration process.

## Dependencies
- Python 3.x
- Required Python libraries (specified in each pipeline file)

## Contributors
- Daniel Chime

Feel free to contribute by submitting pull requests or opening issues.

# DataPipelines-Airflow

## Project Overview
This project focuses on implementing robust, automated data pipelines using Apache Airflow. The objective is to enhance automation, monitoring, and data quality in their data warehouse ETL pipelines.


### Project Requirements
- **Objective:** Create dynamic, reusable data pipelines with monitoring capabilities and support for easy backfills.
- **Data Source:** JSON logs detailing user activity and metadata about songs, residing in S3.
- **Destination:** Sparkify's data warehouse in Amazon Redshift.
- **Tasks:** Develop custom operators for staging data, filling the data warehouse, and running data quality checks.


### Configuring the DAG
1. Set default parameters in the DAG as follows:
    - No dependencies on past runs
    - Retry on failure (3 attempts, 5 minutes interval)
    - Turn off catchup
    - No email notifications on retry
2. Configure task dependencies to follow the provided flow.

### Building Operators
1. **Stage Operator**: Loads JSON files from S3 to Redshift using SQL COPY statements. Parameters distinguish between files and allow timestamp-based backfills.
2. **Fact and Dimension Operators**: Utilize SQL helper class for transformations. Specify SQL statement, target database, and table for results.
    - Dimensions: Employ truncate-insert pattern with an insert mode parameter.
    - Facts: Enable append-only functionality.
3. **Data Quality Operator**: Run SQL-based test cases against data. Compare actual vs. expected outcomes, raise exceptions for discrepancies, and retry until successful.

# DataPipelines-Airflow

## Project Overview
This project focuses on implementing robust, automated data pipelines using Apache Airflow. The objective is to enhance automation, monitoring, and data quality in their data warehouse ETL pipelines.


### Project Requirements
- **Objective:** Create dynamic, reusable data pipelines with monitoring capabilities and support for easy backfills.
- **Data Source:** JSON logs detailing user activity and metadata about songs, residing in S3.
- **Destination:** Sparkify's data warehouse in Amazon Redshift.
- **Tasks:** Develop custom operators for staging data, filling the data warehouse, and running data quality checks.

## Project Details
- **Pipeline Structure:** The project template provides four empty operators that require implementation into functional components of the data pipeline.
- **Custom Operators:** Four operators to be built:
  1. Stage Operator: Loads JSON-formatted files from S3 to Redshift, allowing for templated fields and backfills.
  2. Fact and Dimension Operators: Executes SQL transformations using provided helper class, specifying target tables and databases.
  3. Data Quality Operator: Runs SQL-based test cases against the data, comparing expected results with actual outcomes and raising exceptions for discrepancies.

### Building the Operators
- Utilize Airflow's built-in functionalities (connections, hooks) and optimize the use of parameters for flexibility and reusability across diverse data pipelines.

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from datetime import datetime, timedelta


class DataQualityOperator(BaseOperator):


   ui_color = '#89DA59'


   @apply_defaults
   def __init__(self,
               redshift_conn_id='',
               dq_checks=[],
               *args, **kwargs):


       super(DataQualityOperator, self).__init__(*args, **kwargs)
       self.retries = 3
       self.retry_delay = timedelta(minutes=5)
       self.redshift_conn_id = redshift_conn_id
       self.dq_checks = dq_checks


   def execute(self, context):
       redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
      
       for check in self.dq_checks:
            sql = check.get('check_sql')
            exp_result = check.get('expected_result')
            try:
                self.log.info(f"Running query: {sql}")
                records = redshift_hook.get_records(sql)[0]
            except Exception as e:
                self.log.info(f"Query failed with exception: {e}")
            if exp_result != records[0]:
                error_count += 1
            failing_tests.append(sql)
            if error_count > 0:
                self.log.info('Tests failed')
                self.log.info(failing_tests)
                raise ValueError('Data quality check failed')
            else:
                self.log.info("All data quality checks passed")

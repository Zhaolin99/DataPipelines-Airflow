from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag

from airflow.operators.dummy_operator import DummyOperator
from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator

from airflow.operators.postgres_operator import PostgresOperator
from udacity.common import final_project_sql_statements as sql_
from airflow import DAG 

default_args = {
    'owner': 'airflow-zh',
    'depends_on_past': False,
    'start_date': pendulum.now(),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'email_on_failure': False,
    'catchup': False,

}

with DAG('dag-zh',
         default_args=default_args,
         description='Load and transform data in Redshift with Airflow',
         schedule_interval='@hourly') as dag:

    def final_project():

        create_songs = PostgresOperator(
          task_id="create_songs",
          postgres_conn_id="redshift",
          sql=sql_.create_staging_songs
        )

        create_events = PostgresOperator(
          task_id="create_events",
          postgres_conn_id="redshift",
          sql=sql_.create_staging_events
        )

        create_tables = PostgresOperator(
          task_id="create_tables",
          postgres_conn_id="redshift",
          sql=[sql_.create_songplays,
            sql_.create_artists,
            sql_.create_songs,
            sql_.create_users,
            sql_.create_time,
          ]
        )

        start_operator = DummyOperator(task_id='Begin_execution')
        stage_events_to_redshift = StageToRedshiftOperator(
            task_id='Stage_events',
            redshift_conn_id='redshift',
            aws_credentials_id='aws_credentials',
            table='staging_events',
            s3_bucket='s3://udacity-dend/log_data',
            json_path='s3://udacity-dend/log_json_path.json',
            region = 'us-west-2'
        )
        stage_songs_to_redshift = StageToRedshiftOperator(
            task_id='Stage_songs',
            redshift_conn_id='redshift',
            aws_credentials_id='aws_credentials',
            table='staging_songs',
            s3_bucket='s3://udacity-dend/song_data',
            json_path='auto',
            region = 'us-west-2'
        )


        load_songplays_table = LoadFactOperator(
            task_id='Load_songplays_fact_table',
            redshift_conn_id="redshift",
            table="songplays",
            sql_query=sql_.songplay_table_insert,
            append_data=True
        )


        load_user_dimension_table = LoadDimensionOperator(
            task_id='Load_user_dim_table',
            redshift_conn_id="redshift",
            table="users",
            sql_query=sql_.user_table_insert,
            append_data=True

        )
        load_song_dimension_table = LoadDimensionOperator(
            task_id='Load_song_dim_table',
            redshift_conn_id="redshift",
            table="songs",
            sql_query=sql_.song_table_insert,
            append_data=True

        )
        load_artist_dimension_table = LoadDimensionOperator(
            task_id='Load_artist_dim_table',
            redshift_conn_id="redshift",
            table="artists",
            sql_query=sql_.artist_table_insert,
            append_data=True

        )
        load_time_dimension_table = LoadDimensionOperator(
            task_id='Load_time_dim_table',
            redshift_conn_id="redshift",
            table="time",
            sql_query=sql_.time_table_insert,
            append_data=True
        )

        run_quality_checks = DataQualityOperator(
            task_id='Run_data_quality_checks',
            redshift_conn_id="redshift",
            dq_checks=[
                    { 'check_sql': 'SELECT COUNT(*) FROM public.songplays WHERE userid IS NULL', 'expected_result': 0 }, 
                    { 'check_sql': 'SELECT COUNT(DISTINCT "level") FROM public.songplays', 'expected_result': 2 },
                        ]
        )

        end_operator = DummyOperator(task_id='Stop_execution')

        
        create_songs >>  create_events >> create_tables
        create_tables >> start_operator
        start_operator >> stage_events_to_redshift
        start_operator >> stage_songs_to_redshift
        stage_events_to_redshift >> load_songplays_table
        stage_songs_to_redshift >> load_songplays_table
        load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, \
        load_artist_dimension_table, load_time_dimension_table ] >> run_quality_checks
        run_quality_checks >> end_operator
    final_project_dag = final_project()

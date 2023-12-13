from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

# Define DAG configuration
default_args = {
    'owner': 'Azouz', 
    'depends_on_past': False, # Depend the status of previous run its set in 'false', does not depend on the old execution
    'start_date': days_ago(1), # To calculate a date relative to the current date. It's used to specify when the task should start
    'retries': 1, #This key is associated with the value 1. It defines the number of times the task should be retried if it fails
    'retry_delay': timedelta(minutes=5), #This key is associated with the value timedelta(minutes=5). It specifies the time delay between task retries. In this case, it's set to 5 minutes
}

# Create the DAG
dag = DAG(
    'SG_trigger_snowflake_sql_query',
    default_args=default_args,
    description= 'Step 1 : Trigger ADF to copy files from Internet or Talend to Snowflake'
                 'Step 2 : Trigger ADF to execute Data Transformation with Snowflake Procedure'
                 'Step 2 (Bonus)  : Trigger directly Snowflake for Data Transformation'
                 'Step 3 : Trigger Snowflake procedure to log Airflow execution'
                 'Step 4 : Send an email with the execution status (Failure/Success)',
    schedule_interval=None,  # Set your desired schedule_interval
    catchup=False,  # Disable catchup to prevent backfilling # Disable catchup to prevent backfilling that means schedule and run tasks for all the past dates that the DAG should have run for, based on its schedule interval. This is useful if you want to fill in historical data or rerun tasks for a specific date range.
)

#Define your SQL query
sql_query = """ CALL DEV_RAW_CORP_OPENDATA.ADM.USP_ADM_DW_INSERT_SIRENE_TRAN( 'DEV','RAW_CORP_OPENDATA','DWH_CORP_OPENDATA','RAW','DWH','SIREN_DWH','SIRENE', NULL,'ADMIN','DWH_CORP_OPENDATA');"""

# Task to run procedure into Snowflake: Step 3
run_procedure_to_snowflake = SnowflakeOperator(
    task_id='run_procedure_to_snowflake',
    dag=dag,
    snowflake_conn_id='snowflake_conn',
    autocommit=True,
    sql=sql_query,
    warehouse='WH_DEV_DATA_CORP_OPENDATA',
    database='DEV_DWH_CORP_OPENDATA',
    schema='ADM',
    role='RF_DEV_APP_DATA_CORP_OPENDATA',
)

# Define task dependencies
run_procedure_to_snowflake
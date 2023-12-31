from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime
from airflow.utils.dates import days_ago
from datetime import timedelta

# Define DAG configuration
default_args = {
    'owner': 'Azouz',
    'depends_on_past': False, #Depend the status of previous run its set in 'false', does not depend on the old execution
    'start_date': days_ago(1), #To calculate a date relative to the current date. It's used to specify when the task should start
    'retries': 1, #This key is associated with the value 1. It defines the number of times the task should be retried if it fails
    'retry_delay': timedelta(minutes=5), #This key is associated with the value timedelta(minutes=5). It specifies the time delay between task retries. In this case, it's set to 5 minutes
}

# Define your DAG
dag = DAG(
     'SG_trigger_synapse_pl_2',
     #template_searchpath="./scripts",
     default_args=default_args,
     schedule_interval=None, #Actived option to schedule @hourly, @daily etc
     description='DAG to trigger pipeline into Azure synapses analytics',
     catchup=False,  # Disable catchup to prevent backfilling # Disable catchup to prevent backfilling that means schedule and run tasks for all the past dates that the DAG should have run for, based on its schedule interval. This is useful if you want to fill in historical data or rerun tasks for a specific date range.
)
run_synapse_task = BashOperator(
    task_id='run_azure_synapses_pl',
    bash_command='pwsh -File  /opt/airflow/dags/scripts/synaps.ps1 ',
    dag=dag,
)

# Define your task dependencies if needed
run_synapse_task

from airflow.providers.microsoft.azure.operators.data_factory import AzureDataFactoryRunPipelineOperator
from airflow.utils.dates import days_ago
from datetime import timedelta


# Define DAG configuration
default_args = {
    'owner': 'Azouz',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG
dag = DAG(
    'SG_trigger_adf_PL_synaps_airflow',
    default_args=default_args,
    description='DAG to trigger an Azure Data Factory pipeline with parameter, send an email',
    schedule_interval=None,  # Set your desired schedule_interval
    catchup=False,  # Disable catchup to prevent backfilling
)


# Task to trigger Azure Data Factory pipeline
with dag:
  trigger_pipeline_task = AzureDataFactoryRunPipelineOperator(
     task_id='trigger_pipeline',
     resource_group_name='rg-dataanalytics-dataplatform-dev-westeurope-adf_shir',
     factory_name='adf-dataanalytics-dataplatform-dev-westeurope-bm',
     azure_data_factory_conn_id = "azure_data_factory_conn", # Define your Azure Data Factory credentials
     trigger_rule='all_success',
     pipeline_name='Pl_synapse_call',
     dag=dag,
)

# Define task dependencies
trigger_pipeline_task

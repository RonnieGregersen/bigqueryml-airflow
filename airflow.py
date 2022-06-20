import datetime

from airflow import models
from airflow.operators import bash
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator,BigQueryCreateEmptyTableOperator, BigQueryCreateEmptyDatasetOperator, BigQueryDeleteDatasetOperator

from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

# If you are running Airflow in more than one time zone
# see https://airflow.apache.org/docs/apache-airflow/stable/timezone.html
# for best practices
YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)
PROJECT = "devoteam-gc"

default_args = {
    'owner': 'Composer Example',
    'depends_on_past': False,
    'email': ['ronnie.gregersen@devoteam.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    'start_date': YESTERDAY,
}

createModelQuery = (
        f"""CREATE MODEL `{PROJECT}.initial_load.test_model`
        OPTIONS(MODEL_TYPE='BOOSTED_TREE_CLASSIFIER',
        BOOSTER_TYPE = 'GBTREE',
        NUM_PARALLEL_TREE = 1,
        MAX_ITERATIONS = 50,
        TREE_METHOD = 'HIST',
        EARLY_STOP = FALSE,
        SUBSAMPLE = 0.85,
        INPUT_LABEL_COLS = ['string_field_30'])
        AS SELECT * FROM `{PROJECT}.initial_load.test_materialized_view`;
        """)

with models.DAG(
        'bq_dl',
        catchup=False,
        default_args=default_args,
        schedule_interval=datetime.timedelta(days=1)) as dag:
    
    # Print the dag_run id from the Airflow logs
    print_dag_run_conf = bash.BashOperator(task_id='print_dag_run_conf', bash_command='echo {{ dag_run.id }}')
    create_dataset = BigQueryCreateEmptyDatasetOperator(task_id="create_dataset", dataset_id="initial_load")
    delete_dataset = BigQueryDeleteDatasetOperator(task_id="delete_dataset", dataset_id="initial_load", delete_contents=True)
    create_table = BigQueryCreateEmptyTableOperator(task_id="create_table", dataset_id="initial_load", table_id="test_table")
    #load_data = GoogleCloudStorageToBigQueryOperator(task_id="load_data", bucket="lookerdatasets", source_objects= "gs://lookerdatasets")
    load_csv = GCSToBigQueryOperator(task_id='load_data',bucket='lookerdatasets',source_objects=['*.csv'],destination_project_dataset_table='{PROJECT}.initial_load.test_table',source_format="CSV",create_disposition="CREATE_IF_NEEDED",write_disposition="WRITE_APPEND",autodetect=True,dag=dag)


    create_materialized_view = BigQueryCreateEmptyTableOperator(
        task_id="create_materialized_view",
        dataset_id="initial_load",
        table_id="test_materialized_view",
        materialized_view={
            "query": f"""
            SELECT 
            string_field_4, 
            string_field_7, 
            string_field_8, 
            string_field_10, 
            string_field_11, 
            string_field_12, 
            string_field_13,
            string_field_14, 
            string_field_17, 
            string_field_19, 
            string_field_20, 
            string_field_21, 
            string_field_26, 
            string_field_28,
            string_field_29,
            string_field_30


            FROM `{PROJECT}.initial_load.test_table`

            WHERE string_field_30 != 'Class'



            """,
 

            "enableRefresh": True,
            "refreshIntervalMs": 86400000,
        },
    )

    insert_query_job = BigQueryInsertJobOperator(
        task_id="insert_query_job",
        configuration={
            "query": {
                "query": createModelQuery,
                "useLegacySql": False,
            }
        },
        location="US",
    )

    


    print_dag_run_conf >> delete_dataset >> create_dataset >> create_table >> load_csv >> create_materialized_view >> insert_query_job
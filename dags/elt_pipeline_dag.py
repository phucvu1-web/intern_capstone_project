from datetime import datetime, timedelta
from airflow import DAG
import os
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
import sys

sys.path.insert(0, '/opt/airflow/scripts')

def run_create_tables():
    from create_tables import create_tables

    create_tables()

def run_ingest():
    from ingest import ingest_all
    ingest_all()

default_args = {
    'owner': 'intern',
    'depends_on_past': False,
    'retries': 1,
    
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='elt_pipeline_dag',
    default_args=default_args,
    schedule=None,
    start_date=datetime(2024,1,1),
    catchup=False,
    tags=['elt', 'pet', 'dbt'],
) as dag:
    
    create_tables = PythonOperator(
        task_id = 'create_tables',
        python_callable=run_create_tables,
    )

    ingest_data = PythonOperator(
        task_id = 'ingest_data',
        python_callable=run_ingest,
    )
    
    run_dbt = DockerOperator(
        task_id = 'run_dbt_transformations',
        image = 'intern_capstone_project-dbt',
        api_version='auto',
        auto_remove=True,
        command='bash -c "dbt run --project-dir /usr/dbt/project && dbt test --project-dir /usr/dbt/project"',
        docker_url='unix:///var/run/docker.sock',
        network_mode = 'intern_capstone_project_pipeline_net',
        environment={
            'PGHOST': 'postgres_warehouse',
            'PGPORT': '5432',
            'PGUSER': 'postgres_warehouse',
            'PGPASSWORD': 'postgres_warehouse',
            'PGDATABASE': 'postgres_warehousedb',           
        },
        mounts=[
            {
                'source': '/home/phuc.vu1/Downloads/intern_capstone_project/dbt_project',
                'target': '/usr/dbt/project',
                'type': 'bind',
            }
        ],
    )

    create_tables >> ingest_data >> run_dbt
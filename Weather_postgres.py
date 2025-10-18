from airflow.sensors.external_task_sensor import ExternalTaskSensor
from psycopg2.extras import execute_values
from botocore.exceptions import ClientError
import psycopg2
from airflow.operators.python import get_current_context
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.email import EmailOperator
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta, timezone
from sqlalchemy import create_engine
from io import StringIO
from airflow import DAG
import pandas as pd
import psycopg2
import pytz
import json
import yaml 
import os

Dataset=["current", "forecast", "hourly", "pollution", "uv"]

@dag(
    dag_id="postgres_upload",
    schedule_interval='50 11 * * 0',
    catchup=False,
    start_date=datetime(2025, 7, 9),
    default_args={
        'owner':'airflow',
        'retries':0,
        'retry_delay':timedelta(minutes=2)
    },
    tags=["processed", "upload", "dashboard"],
    default_view="graph",
    description="Output files are storing in postgres database for Dashboard purpose",
)
def postgres_dag():
    @task()
    def get_today_date():
        IST = timezone(timedelta(hours=5, minutes=30))
        today = datetime.now(IST).strftime('%Y%m%d')
        return today
        # today='20250918'
        # return today
    
    @task
    def get_processing_monday_folder(current_date):
        current_date_obj = datetime.strptime(current_date, '%Y%m%d')
        monday=current_date_obj - timedelta(days=current_date_obj.weekday())
        monday_str=monday.strftime('%Y%m%d')
        return monday_str
        # monday='20250728'
        # return monday

    @task()
    def get_active_dataset():
        ctx = get_current_context()
        dataset = ctx["dag_run"].conf.get("dataset","all")

        if dataset  not in Dataset and dataset!='all':
            raise ValueError(f"Invalid dataset parameter {dataset}")
        if dataset == "all":
            dataset_to_fetch=Dataset[:]
        else:
            dataset_to_fetch=[dataset]
        
        return dataset_to_fetch

    @task
    def get_postgres_config_file(active_ds):
        pt_config={}
        bucket_name="weatherbucket-yml"
        aws_conn_id="weather_bucket_s3_conn"
        for ds in active_ds:

            key=f"weather_data/Automation/configuration/{ds}/Postgres/postgres_config_{ds}.yml"
            s3=S3Hook(aws_conn_id=aws_conn_id)
            content=s3.read_key(bucket_name=bucket_name,key=key)
            config=yaml.safe_load(content)
            pt_config[ds]=config

        return pt_config
    

    
    @task
    def creating_postgres_schema(pt_config):

        for ds, config in pt_config.items():

            for rule in config['configuration']:
                rule_type=list(rule.keys())[0]
                rule_config=rule[rule_type]

                if rule_type=='postgres_schema':
                    pg_config=rule_config
                    
                    conn_id=pg_config['connection_id']
                    database=pg_config['database']
                    table= pg_config['table']
                    schema= pg_config['schema']

                    columns_defs=[]
                    for column in schema:
                        col_name=column['name']
                        col_type=column['type']
                        if not col_name or not col_type:
                            raise ValueError(
                                f"❌ Invalid column definition in dataset `{ds}`: {column}"
                            )
                        columns_defs.append(f"{col_name} {col_type}")
                    
                    create_table_query = f"""
                        CREATE TABLE IF NOT EXISTS {table} (
                            {', '.join(columns_defs)}
                        );
                        """
                    

                    conn_info = BaseHook.get_connection(conn_id)

                    try:
                        conn = psycopg2.connect(
                            host=conn_info.host,
                            port=conn_info.port,
                            user=conn_info.login,
                            password=conn_info.password,
                            dbname=database
                            
                        )
                        cur = conn.cursor()
                        cur.execute(create_table_query)
                        conn.commit()
                        cur.close()
                        conn.close()
                        print(f"✅ Table `{table}` created or already exists in `{database}`.")
                    except Exception as e:
                        raise RuntimeError(f"❌ Failed to create table `{table}`: {e}")
                
    @task
    def reading_the_csv_as_df(processing_week,pt_config, current_date):

        dfs={}
        for ds, config in pt_config.items():
            for rule in config['configuration']:
                rule_type=list(rule.keys())[0]
                rule_config=rule[rule_type]

                if rule_type=='output_file_location':
                    bucket_name=rule_config['bucket_name']
                    file_path=rule_config['file_path']
                    file_name=rule_config['file_name']
                    aws_conn_id=rule_config['aws_conn_id']

                    processing_file_path=file_path.replace("[processing_week]",processing_week)
                    processing_file_name=file_name.replace("[current_date]", current_date)
                    file_key=os.path.join(processing_file_path,processing_file_name)

                    s3=S3Hook(aws_conn_id=aws_conn_id)
                    try:
                        content = s3.read_key(bucket_name=bucket_name, key=file_key)
                        df = pd.read_csv(StringIO(content))
                        dfs[ds] = df.to_json(orient="records")

                    except ClientError as e:
                        raise RuntimeError(
                            f"Failed to read S3 object. "
                            f"Bucket: {bucket_name}, Key: {file_key}, Error: {str(e)}"
                        )
                    except Exception as e:
                        raise RuntimeError(
                            f"Unexpected error while processing file. "
                            f"Bucket: {bucket_name}, Key: {file_key}, Error: {str(e)}"
                        )
        return dfs
    
    @task()
    def uploading_data_to_postgres(pt_config,dfs):
        for ds, df_json in dfs.items():
            df = pd.read_json(df_json, orient='records')

        # Initialize config variables
            config = pt_config[ds]
            conn = None
            conn_id = None
            database = None
            table = None
            upsert_config= None

        # Parse YAML configuration
            for rule in config['configuration']:
                rule_type = list(rule.keys())[0]
                rule_config = rule[rule_type]

                if rule_type == 'postgres_schema':
                    conn_id = rule_config['connection_id']
                    database = rule_config['database']
                    table = rule_config['table']
                    schema_config = rule_config.get('schema', [])
                    conn = BaseHook.get_connection(conn_id)
            

            # Validate required config
            if None in [conn_id, database, table, conn]:
                raise ValueError("Missing required PostgreSQL configuration in YAML file.")

            # Create SQLAlchemy engine
            conn_str = f"postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{database}"
            engine = create_engine(conn_str)

            # --- Load data into Postgres (append mode) ---
            with engine.begin() as connection:
                connection.execute(f"TRUNCATE TABLE {table};")

         
                df.to_sql(table, connection, index=False, if_exists="append", method="multi")
    
    current_date= get_today_date()
    processing_week = get_processing_monday_folder(current_date)
    active_ds=get_active_dataset()
    config = get_postgres_config_file(active_ds)
    schema=creating_postgres_schema(config)
    df_json = reading_the_csv_as_df(processing_week, config, current_date)
    upload= uploading_data_to_postgres(config, df_json)

    config >> schema
    [config, processing_week] >> df_json
    [config, df_json] >> upload

postgres_dag()


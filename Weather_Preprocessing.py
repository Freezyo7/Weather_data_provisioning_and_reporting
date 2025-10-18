from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.operators.python import get_current_context
from airflow.operators.python import BranchPythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.email import EmailOperator
from airflow.operators.empty import EmptyOperator
from airflow.decorators import dag, task
from datetime import datetime, timedelta, timezone
from airflow import DAG
from io import StringIO
import logging
from jsonpath_ng import parse                                
import pandas as pd
import pytz
import json
import yaml 
import os

DATASET=["current", "hourly", "forecast", "pollution", "uv"]

@dag(
    dag_id="weather_dag_preprocessing",
    schedule_interval="20 0 * * *",
    start_date=datetime(2025, 7 ,9),
    catchup=False,
    default_args={
        'owner': 'airflow',
        'retries' : 0,
        'retry_delay': timedelta(minutes=2)
    },
    tags=["extract", "transform", "weekly"],
    default_view="graph",
    description="transform raw data and merge them together weekly",
    doc_md="""
    ###preprocessing
     """
) 

def pre_processing_dag():

    @task()
    def create_monday_folder():
        IST = timezone(timedelta(hours=5, minutes=30))
        today = datetime.now(IST)
        monday = today - timedelta(days=today.weekday())
        return monday.strftime('%Y%m%d')
        # monday="20250818"
        # return monday

    @task()
    def get_active_dataset():
        ctx = get_current_context()
        dataset = ctx["dag_run"].conf.get("dataset","all")

        if dataset  not in DATASET and dataset!='all':
            raise ValueError(f"Invalid dataset parameter {dataset}")
        if dataset == "all":
            dataset_to_fetch=DATASET[:]
        else:
            dataset_to_fetch=[dataset]
        
        return dataset_to_fetch


    @task()
    def get_PLparameter(active_ds):
        all_pp={}

        bucket_name="weatherbucket-yml"
        aws_conn_id="weather_bucket_s3_conn"
        s3=S3Hook(aws_conn_id=aws_conn_id)

        for s in active_ds:
            key=f"weather_data/Automation/configuration/{s}/pl_parameter/pipeline_parameter_{s}.json"
            content=s3.read_key(bucket_name=bucket_name,key=key)
            all_pp[s]=json.loads(content)
        return all_pp
    
    @task
    def get_preprocessing_trans_file (all_pp):
        all_pp_etl={}
        for dataset, pp in all_pp.items():
            bucket_name=pp["bucket_name"]
            aws_conn_id=pp["aws_conn_id"]
            s3=S3Hook(aws_conn_id=aws_conn_id)
            key=pp["current_transformation_file_loc"]
            content=s3.read_key(bucket_name=bucket_name,key=key)
            config=yaml.safe_load(content)

            all_pp_etl[dataset]=config
        return all_pp_etl

    
    @task()
    def transformation_current (pp,pp_etl,folder,rolling_days=6):
        IST = timezone(timedelta(hours=5, minutes=30))
        today_str=datetime.now(IST).strftime("%Y%m%d")
        # today_str="20250827"
        today_dt = datetime.now(IST).date()
        week_end = today_dt + timedelta(days=rolling_days)
        
        logging.info(f"Filtering data from {today_dt} to {week_end}")
        
        dataset_to_data={}
        for dataset, pl in pp.items():
            aws_conn_id=pl["aws_conn_id"]
            s3=S3Hook(aws_conn_id=aws_conn_id)

            bucket_name=pl["bucket_name"]
            source_path=pl["source_path"]
            source_file_name=pl["source_file_name"].format(today=today_str)
            source_file_extension=pl["source_file_extension"]


            prefix=f"{source_path}{folder}/"
            expected_file=f"{source_file_name}{source_file_extension}"

            keys=s3.list_keys(bucket_name=bucket_name,prefix=prefix)
            if not keys:
                raise FileNotFoundError(f"No files found in {prefix}") 
            
            today_key=None
            for k in keys:
                if k.endswith(expected_file):
                    today_key=k
                    break
            if not today_key:
                raise FileNotFoundError(f"Missing file {expected_file}")
            
            raw_content= s3.read_key(bucket_name=bucket_name,key=today_key)
            raw_data=json.loads(raw_content)
            
            dataset_config= pp_etl[dataset]["datasets"][dataset]
            field_map= dataset_config["mappings"]
            iterate_mode=dataset_config["iterate_over"]

            logging.info(f"Processing dataset {dataset} with iterate_mode {iterate_mode}")

            city_path=dataset_config.get("city_path",None)
            root_path=dataset_config.get("root_path",None)

            def extract_fields(data, field_map):
                row={}
                for out_field, jsonpath_expr in field_map.items():
                    try:
                        parse_expr=parse(jsonpath_expr)

                        matches=[]
                        for match in parse_expr.find(data):
                            matches.append(match.value)

                        if len(matches)>0:
                            row[out_field]=matches[0]
                        else:
                            row[out_field]=None

                    except Exception as e:
                        print(f"Error extracting field '{out_field}': {e}")
                        row[out_field]=None
                return row
            
            storing_data=[]
            for city_key,data in raw_data.items():
                if not data:
                    continue
                
                if iterate_mode=="days":
                    parsed_root_path=parse(root_path)
                    matches=parsed_root_path.find(data)
                    if not matches:
                        continue
                    for match in matches:
                        day=match.value
                        row=extract_fields(day, field_map)
                        row["city"]=city_key
                        storing_data.append(row)
                
                elif iterate_mode=="list_items":
                    parsed_root_path = parse(root_path)
                    matches = parsed_root_path.find(data)

                    if not matches:
                        continue
                    
                    run_date_str = today_dt.strftime("%Y-%m-%d")
                    # run_date_str="2025-08-27"
                    for match in matches:
                        item=match.value
                        if "dt" in item:
                            ts = item["dt"]
                            IST = timezone(timedelta(hours=5, minutes=30))
                            ts_date = datetime.fromtimestamp(ts, IST).strftime("%Y-%m-%d")
                            if ts_date!=run_date_str:
                                continue
                        row=extract_fields(item, field_map)
                        row["city"]=city_key
                        storing_data.append(row)

                elif iterate_mode == "seven_days":

                    parsed_root_path=parse(root_path)
                    matches=parsed_root_path.find(data)
                    logging.info(f"[seven_days] Found {len(matches)} matches for city {city_key}")

                    if not matches:     
                        continue
                    
                    for match in matches:
                        fore=match.value
                        logging.info(f"[seven_days] Processing item: {fore}")
                        if "datetime" in fore:
                            
                            ts_date = datetime.strptime(fore["datetime"], "%Y-%m-%d").date()
                            
                            if not (today_dt <= ts_date <= week_end):
                                logging.info(f"[seven_days] Skipping date {ts_date} outside range")
                                continue

                        row=extract_fields(fore, field_map)
                        row["city"]=city_key
                        storing_data.append(row)
                        
                elif iterate_mode=="hourly_data":
                    parsed_root_path = parse(root_path)
                    matches = parsed_root_path.find(data)
                    

                    uv_index_max = None
                    if "daily" in data and "uv_index_max" in data["daily"]:
                        if isinstance(data["daily"]["uv_index_max"], list) and len(data["daily"]["uv_index_max"]) > 0:
                            uv_index_max = data["daily"]["uv_index_max"][0]

                    for match in matches:
                        
                        if "hourly" not in data:
                            continue

                        hourly_data=data["hourly"]

                        time_list=hourly_data["time"]

                        for i in range(len(time_list)):
                            timestamp=time_list[i]  
                            combined_data={
                                "time":timestamp
                            }

                            for key,values in hourly_data.items():
                                if key=="time":
                                    continue
                                if isinstance(values,list) and len(values)>i:
                                    combined_data[key]=values[i]
                                else:
                                    combined_data[key]=None
                                    
                            combined_data["uv_index_max"] = uv_index_max

                            row = extract_fields(combined_data, field_map)
                            row["city"] = city_key
                            storing_data.append(row)
            
            df=pd.DataFrame(storing_data)
            filename=pl["preprocessing_file_name"].format(today=today_str)
            ext=pl["preprocessing_file_extension"]
            csv_file=f"/tmp/{filename}{ext}"
            df.to_csv(csv_file,index=False)

            dataset_to_data[dataset]=csv_file
                
        return dataset_to_data
    
    @task
    def upload_to_s3(dataset_to_data,folder,pp):
        # today=datetime.utcnow().strftime('%Y%m%d')
        # today="20250812"
        
        for d,file_path in dataset_to_data.items():
            s=pp[d]
            bucket_name=s["bucket_name"]
            aws_conn_id=s["aws_conn_id"]
            s3=S3Hook(aws_conn_id=aws_conn_id)

            filename=os.path.basename(file_path)
            ext=s["preprocessing_file_extension"]
            loc=s["preprocessing_storage_path"]
            
            s3.load_file(
                filename=file_path,
                key=f"{loc}{d}/{folder}/{filename}",
                bucket_name=bucket_name,
                replace=True
            )
    
    folder = create_monday_folder()
    active_ds=get_active_dataset()
    pp = get_PLparameter(active_ds)
    pp_etl = get_preprocessing_trans_file(pp)
    dataset_to_data = transformation_current(pp, pp_etl, folder)
    upload_to_s3(dataset_to_data=dataset_to_data, folder=folder, pp=pp)

pre_processing_dag()
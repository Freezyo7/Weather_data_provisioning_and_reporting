from airflow.sensors.external_task_sensor import ExternalTaskSensor
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
import pandas as pd
import logging
import numpy as np
import pytz
import json
import yaml 
import os
import io


DATASET=["current", "hourly", "forecast", "pollution", "uv"]

@dag(
    dag_id="weather_dag_etl",
    schedule_interval="40 11 * * *",
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
    ###ETl
     """
)
def etl_dag():
    @task()
    def get_today_date():
        IST = timezone(timedelta(hours=5, minutes=30))
        today = datetime.now(IST).strftime('%Y%m%d')
        return today
        # today='20250918'
        # return today
    
    @task()
    def get_processing_monday_folder(current_date):
        current_date_obj = datetime.strptime(current_date, '%Y%m%d')
        monday=current_date_obj - timedelta(days=current_date_obj.weekday())
        monday_str=monday.strftime('%Y%m%d')
        return monday_str
        # monday="20250818"
        # return monday
    
    
    @task()
    def get_dataset_config_file():
        bucket_name="weatherbucket-yml"
        key="weather_data/Automation/dynamic/dataset_configuration.xlsx"
        aws_conn_id="weather_bucket_s3_conn"

        s3=S3Hook(aws_conn_id=aws_conn_id)
        obj=s3.get_key(bucket_name=bucket_name, key=key)
        content=obj.get()['Body'].read()
        ds=pd.read_excel(io.BytesIO(content),engine="openpyxl")
        return ds.to_dict(orient="records")
    

    @task()
    def get_active_dataset_config_file(dataset_config):

        df=pd.DataFrame(dataset_config)
        df['Status'] = df['Status'].astype(str).str.strip().str.lower()

        active_df = df[df['Status'] == 'active']

        return active_df.to_dict(orient="records")


    @task()
    def get_yml_file(active_ds_config:dict):
        etl_info={}

        bucket_name="weatherbucket-yml"
        aws_conn_id="weather_bucket_s3_conn"
        s3=S3Hook(aws_conn_id=aws_conn_id)

        for ds in active_ds_config:
            key=ds["ETL_file_path"]

            keys=s3.list_keys(bucket_name=bucket_name, prefix=key)
            if not keys:
                raise FileNotFoundError(f"❌ Key not found in bucket {bucket_name}: {key}")
            content=s3.read_key(bucket_name=bucket_name,key=key)
            config=yaml.safe_load(content)
            etl_info[ds["Dataset"]]=config

        return etl_info
    
    
    @task()
    def etl_transformation(yml_file,processing_week, current_date):

        results={}# ds:info
        for ds, info in yml_file.items():
            #dataframe name
            df_map={} 

            for rule in info['transform_rules']:
                rule_type=list(rule.keys())[0]
                rule_config=rule[rule_type]

                if rule_type=='add_dataframe':
                    add_df_rule=rule_config
                    df_name=add_df_rule['dataframe']

                    raw_file_path= add_df_rule['file_path']
                    resolved_path=raw_file_path.replace('{processing_week}',processing_week)

                    bucket_name=resolved_path.replace('s3://','').split('/')[0]
                    prefix = "/".join(resolved_path.replace('s3://','').split('/')[1:])


                    s3=S3Hook(aws_conn_id="weather_bucket_s3_conn")
                    all_keys=s3.list_keys(bucket_name=bucket_name, prefix=prefix)
                    logging.info(f"[ETL] Looking in s3://{bucket_name}/{prefix}")
                    logging.info(f"[ETL] Available keys: {all_keys}")
                    
                    #--------only pick the current date file----------
                    file_hint = add_df_rule.get("file_name_hint","")
                    if file_hint:
                        expected_substring = file_hint.replace("{current_date}", current_date)
                        logging.info(f"[ETL] file_name_hint provided in YAML: {file_hint}")
                        logging.info(f"[ETL] After replacing current_date={current_date}, expected_substring = {expected_substring}")
                    else:
                        expected_substring = current_date
                        logging.info(f"[ETL] No file_name_hint in YAML, falling back to current_date = {expected_substring}")

                    csv_keys = []
                    for key in all_keys or []:
                        if key.endswith(".csv") and expected_substring in key:
                            csv_keys.append(key) 

                    if not csv_keys:
                        raise ValueError(
                            f"No CSV files found in s3://{bucket_name}/{prefix} "
                            f"with hint '{file_hint}' for processing_week={processing_week}. "
                            f"Available keys: {all_keys}"
                            )
                    
                    key = csv_keys[0]
                    logging.info(f"[ETL] Using file: s3://{bucket_name}/{key}")

                    obj = s3.read_key(bucket_name=bucket_name, key=key)
                    if not obj.strip():
                        raise ValueError(f"S3 file {key} is empty!")

                    df = pd.read_csv(StringIO(obj))
                    df_map[df_name] = df
                    logging.info(f"[ETL] Added dataframe '{df_name}' with shape {df.shape}")

                elif rule_type=='drop_duplicate':
                    df_name=rule_config['dataframe']
                    df=df_map[df_name]

                    subset=rule_config.get('subset', None)
                    keep=rule_config.get('keep','first')
                    if subset:
                        if isinstance(subset, str):
                            subset = [subset]
                        subset = [col for col in subset if col in df.columns]
                        if not subset:
                            subset = None

                    df.drop_duplicates(subset=subset, keep=keep, inplace=True)

                    df_map[df_name]=df

                elif rule_type=="dropna":
                    df_name=rule_config['dataframe']
                    df=df_map[df_name]

                    subset = rule_config.get("subset", None)
                    how = rule_config.get("how", "any")
                    thresh = rule_config.get("thresh", None)
                    if subset:
                        # If single column is a string, convert to list
                        if isinstance(subset, str):
                            subset = [subset]
                        # Keep only columns that exist in df
                        subset = [col for col in subset if col in df.columns]
                        if not subset:
                            subset = None

                    df.dropna(subset=subset,how=how, thresh=thresh, inplace=True)

                    df_map[df_name]=df

                elif rule_type=='date_formatting':
                    df_name=rule_config['dataframe']
                    df=df_map[df_name]

                    timezone_column=rule_config.get('timezone_column', None)

                    columns=rule_config['columns']
                    for col in columns:
                        source_col=col['source']
                        target_col=col['target']
                        fmt=col['format']
                        input_type=col.get('input_type', None)
                        
                        if input_type == "iso":
                            # ISO datetime strings
                            df[target_col] = pd.to_datetime(df[source_col]).dt.strftime(fmt)
                        elif input_type == "uv":
                            df[target_col] = pd.to_datetime(
                                df[source_col], format="%Y-%m-%dT%H:%M", errors="coerce"
                            ).dt.strftime(fmt)

                        else:
                            # Epoch timestamps
                            if timezone_column and timezone_column in df.columns:
                                df[target_col] = (
                                    df[source_col].astype(int) + df[timezone_column].astype(int)
                                ).apply(lambda ts: datetime.utcfromtimestamp(ts).strftime(fmt))
                            else:
                                # Default to IST conversion
                                df[target_col] = pd.to_datetime(df[source_col].astype(int), unit="s", utc=True) .dt.tz_convert("Asia/Kolkata").dt.strftime(fmt)

                    df_map[df_name] = df
                
                elif rule_type=='country_mappings':
                    df_name=rule_config['dataframe']
                    df=df_map[df_name]

                    column=rule_config['column']
                    mapping=rule_config['mapping']

                    df[column]=df[column].map(mapping)

                    df_map[df_name]=df
                
                elif rule_type=='convert_temperature':
                    df_name=rule_config['dataframe']
                    df=df_map[df_name]

                    from_unit=rule_config['from_unit']
                    to_unit=rule_config['to_unit']
                    columns=rule_config['columns']

                    if from_unit=='kelvin' and to_unit=='celcius':
                        for col in columns:
                            df[col]=(df[col]-273.15).round(2)

                    df_map[df_name]=df
                
                elif rule_type=='drop_columns':
                    df_name=rule_config['dataframe']
                    df=df_map[df_name]

                    columns=rule_config['columns']
                    df.drop(columns=columns, inplace=True, errors='ignore')

                    df_map[df_name]=df

                elif rule_type=='handle_missing':
                    df_name=rule_config['dataframe']
                    df=df_map[df_name]

                    method=rule_config['method']
                    fill_val=rule_config['fill_value']

                    if method=="drop":
                        df.dropna(inplace=True)
                    elif method=="fill":
                        df.fillna(value=fill_val,inplace=True)
                    
                    df_map[df_name]=df
                    
                elif rule_type=='renaming_columns':
                    df_name=rule_config['dataframe']
                    df=df_map[df_name]

                    rename_column=rule_config['rename_columns']
                    df.rename(columns=rename_column,inplace=True)

                    df_map[df_name]=df
                
                elif rule_type=='reorder_columns':
                    df_name=rule_config['dataframe']
                    df=df_map[df_name]

                    column_order=rule_config['columns']
                    reordered_column=[]
                    for col in column_order:
                        if col in df.columns:
                            reordered_column.append(col)
                    df=df[reordered_column]

                    df_map[df_name]=df
                
                elif rule_type=="type_casting":
                    df_name=rule_config['dataframe']
                    df=df_map[df_name]

                    data_type=rule_config['dtype_mappings']
                    for col, val in data_type.items():
                        df[col]=df[col].astype(val)
                    
                    df_map[df_name]=df
                
                elif rule_type == "derived_columns":
                    df_name = rule_config['dataframe']
                    df = df_map[df_name]
                    rule = rule_config['rules']

                    for col_name, expression in rule.items():
                        print("Columns available:", df.columns.tolist())
                        try:
                            df[col_name] = df.eval(expression)
                        except Exception:
                            
                            df[col_name] = eval(expression, {"np": np, "pd": pd}, {"df": df})


                    df_map[df_name] = df
                
                elif rule_type == 'interpolate_columns':
                    df_name = rule_config['dataframe']
                    df = df_map[df_name]

                    method = rule_config.get('method', 'linear')
                    columns = rule_config.get('columns', [])

                    for col in columns:
                        if col in df.columns:
                            df[col] = df[col].interpolate(method=method)

                    df_map[df_name] = df

                elif rule_type == 'resample_dataframe':
                    df_name = rule_config['dataframe']
                    df = df_map[df_name]

                    datetime_col = rule_config['datetime_col']
                    freq = rule_config.get('freq', '1H')

                    if datetime_col not in df.columns:
                        raise ValueError(f"Column {datetime_col} not found in dataframe {df_name}")
                    
                    df[datetime_col] = pd.to_datetime(df[datetime_col], errors="coerce")

                    def resample_city(g):
                        g=g.set_index(datetime_col).resample(freq).asfreq()
                        first_ts = g.index.min()  
                        start= first_ts.floor("H") 
                        end = start + pd.Timedelta(hours=23)
                        return g.loc[start: end]

                    df = (
                        df.groupby("city", group_keys=False)
                        .apply(resample_city)
                        .reset_index()
                    )

                    df_map[df_name] = df
                
                elif rule_type == 'forward_fill_columns':
                    df_name = rule_config['dataframe']
                    df = df_map[df_name]

                    columns = rule_config.get('columns', [])
                    for col in columns:
                        if col in df.columns:
                            df[col] = df[col].ffill()

                    df_map[df_name] = df

            for df_name, df in df_map.items():
                file_name = f"etl{ds}_weather_data.csv"
                local_path = os.path.join("/tmp", file_name)
                df.to_csv(local_path, index=False)
                results[ds]=local_path

        return results

    @task()
    def store_transformed_data(results,processing_week, active_dataset_config, current_date):
        s3=S3Hook(aws_conn_id="weather_bucket_s3_conn")
        uploaded_files=[]
        
        for info in active_dataset_config:
            dataset=info["Dataset"]
            local_path=results.get(dataset)
            if not local_path:
                raise ValueError(f"No transformed file found for dataset: {dataset}")
            
            file_name=info["Output_file_name"].format(current_date=current_date)
            output_path=info["Output_path"].format(processing_week=processing_week)
            output_key=f"{output_path}{file_name}"

            s3.load_file(
                filename=local_path,
                key=output_key,
                bucket_name="weatherbucket-yml",
                replace=True
            )
            os.remove(local_path)
            uploaded_files.append(output_key)

        return uploaded_files
    

    send_success_email = EmailOperator(
        task_id='send_success_email',
        to='nishantsingh27022004@gmail.com',
        subject='✅ Weather DAG ETL Succeeded!',
        html_content="""
            <h2 style="color:green;">Weather Data Acquisition Pipeline Success</h2>
            <p><b>DAG:</b> weather_dag</p>
            <p><b>Execution Time:</b> {{ execution_date }}</p>
            <p><b>Generated CSV Files:</b></p>
            <ul>
            {% set files = ti.xcom_pull(task_ids='etl_transformation') %}
            {% for ds, path in files.items() %}
                <li><b>{{ ds }}</b>: {{ path.split('/')[-1] }} (Local: {{ path }})</li>
            {% endfor %}
            </ul>
            <p><b>S3 Upload Folder:</b> {{ ti.xcom_pull(task_ids='store_transformed_data') or 'Unknown' }}</p>
            <br>
            <p style="font-size:14px;">Check your S3 bucket for the updated weather data.</p>
            <hr>
            <p style="font-size:13px;color:gray;">Airflow DAG Run ID: {{ run_id }}</p>
        """,
        trigger_rule=TriggerRule.ALL_SUCCESS
        )

    send_failure_email = EmailOperator(
        task_id='send_failure_email',
        to='nishantsingh27022004@gmail.com',
        subject='❌ Weather DAG ETL Failed!',
        html_content="""
            <h2 style="color:red;">Weather Data Acquisition Pipeline Failure</h2>
            <p><b>DAG:</b> weather_dag</p>
            <p><b>Execution Time:</b> {{ execution_date }}</p>
            <p>One or more tasks in the <b>weather_dag</b> have failed.</p>
            <p>Please review the logs in the Airflow UI for details.</p>
            <br>
            <p><b>Run ID:</b> {{ run_id }}</p>
            <p><b>Failed Task:</b> {{ task_instance.task_id }}</p>
            <p><b>Try Number:</b> {{ task_instance.try_number }}</p>
            <hr>
            <p style="font-size:13px;color:gray;">Visit the <a href="{{ ti.log_url }}">Airflow Logs</a> for this task.</p>
        """,
        trigger_rule=TriggerRule.ONE_FAILED
        )

    current_date = get_today_date()
    processing_week = get_processing_monday_folder(current_date)
    dataset_config = get_dataset_config_file()
    active_config = get_active_dataset_config_file(dataset_config)
    yml_file = get_yml_file(active_config)
    results = etl_transformation(yml_file, processing_week, current_date)
    upload=store_transformed_data(results, processing_week, active_config, current_date)

    upload >> [send_failure_email, send_success_email]
     
etl_dag()

# @task()
#     def etl_transformation(yml_file,processing_week):

#         results={}
#         for ds, info in yml_file.items():
#             df_map={}

#             for rule in info['transform_rules']:
#                 rule_type=list(rule.keys())[0]
#                 rule_config=rule[rule_type]

#                 if rule_type=='add_dataframe':
#                     add_df_rule=rule_config
#                     df_name=add_df_rule['dataframe']

#                     raw_file_path= add_df_rule['file_path']
#                     resolved_path=raw_file_path.replace('{processing_week}',processing_week)
#                     bucket_name=resolved_path.replace('s3://','').split('/')[0]
#                     prefix = "/".join(resolved_path.replace('s3://','').split('/')[1:])


#                     s3=S3Hook(aws_conn_id="weather_bucket_s3_conn")
#                     all_keys=s3.list_keys(bucket_name=bucket_name, prefix=prefix)
#                     print(f"DEBUG: Looking for files in s3://{bucket_name}/{prefix}")
#                     print(f"DEBUG: Available keys: {all_keys}")
#                     print(f"DEBUG: file_name_hint = {add_df_rule.get('file_name_hint','')}")
                    
#                     csv_keys = []
#                     file_hint = add_df_rule.get("file_name_hint","")
#                     for key in all_keys or []:
#                         if key.endswith(".csv") and (file_hint in key if file_hint else True):
#                             csv_keys.append(key) 

#                     if not csv_keys:
#                         raise ValueError(
#                             f"No CSV files found in s3://{bucket_name}/{prefix} "
#                             f"with hint '{file_hint}' for processing_week={processing_week}. "
#                             f"Available keys: {all_keys}"
#                             )
#                     df_list=[]
#                     for key in csv_keys:
#                         obj=s3.read_key(bucket_name=bucket_name, key=key)
#                         if not obj.strip():
#                             raise ValueError(f"S3 file {key} is empty!")

#                         df = pd.read_csv(StringIO(obj))
#                         df=pd.read_csv(StringIO(obj))
#                         df_list.append(df)

#                     df=pd.concat(df_list,ignore_index=True)
#                     df_map[df_name]=df
#                     print(f"DEBUG: Added dataframe '{df_name}' with shape {df.shape}")

    # @task()
    # def count_no_of_file(yml_file,processing_week):

    #     start_date=datetime.strptime(processing_week, '%Y%m%d')
        
    #     expected_filename=[]
    #     for i in range(7):
    #         day= start_date+ timedelta(days=i)
    #         filename=f"weather_{day.strftime('%Y%m%d')}" + '.csv'
    #         expected_filename.append(filename)


    #     for rule in yml_file['transform_rules']:
    #         rule_type=list(rule.keys())[0]
    #         rule_config=rule[rule_type]
    #         if rule_type=='add_dataframe':
    #             file_path=rule_config['file_path']
    #             input_file_path=file_path.replace('[processing_week]',processing_week)
    #             bucket_name=input_file_path.replace('s3://','').split('/')[0]
    #             prefix="/".join(input_file_path.replace('s3://','').split('/')[1:])

    #             s3=S3Hook(aws_conn_id="weather_bucket_s3_conn")
    #             all_keys=s3.list_keys(bucket_name=bucket_name, prefix=prefix)

    #             file_key=[]
    #             if all_keys is not None:
    #                 for key in all_keys:
    #                     if not key.endswith('/'):
    #                         file_key.append(key)
                
    #             found_filename=[]
    #             for key in file_key:
    #                 f_name=key.split('/')[-1]
    #                 found_filename.append(f_name)
                
    #             missing_filename=[]
    #             for name in expected_filename:
    #                 if name not in found_filename:
    #                     missing_filename.append(name)
    #             return missing_filename
    #     return expected_filename
    
    # @task.branch()
    # def check_if_missing(missing_files):
    #     if missing_files:
    #         return 'send_missing_file_email'
    #     return 'etl_transformation'

    # send_missing_file_email = EmailOperator(
    # task_id='send_missing_file_email',
    # to='nishantsingh27022004@gmail.com',
    # subject='❌ Missing Weather Files',
    # html_content="""<h3 style="color:red;">Missing Files Detected</h3>
    # <p><b>Missing Files:</b><br>{{ ti.xcom_pull(task_ids='count_no_of_file') }}</p>""",
    # trigger_rule=TriggerRule.ALL_DONE
    # )

# import os
# import pandas as pd
# import json
# import yaml 
# from datetime import datetime, timedelta
# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from airflow.providers.amazon.aws.hooks.s3 import S3Hook
# from airflow.operators.email import EmailOperator
# from io import StringIO
# import pytz
# from airflow.sensors.external_task_sensor import ExternalTaskSensor

# def get_yml_file(**kwargs):
#     task_instance=kwargs['ti']
#     bucket_name = "weatherbucket-yml"
#     key = "weather_data/configuration/etl.yml"
#     aws_conn_id = "weather_bucket_s3_conn"

#     s3=S3Hook(aws_conn_id=aws_conn_id)
#     content=s3.read_key(bucket_name=bucket_name,key=key)

#     config = yaml.safe_load(content)

#     task_instance.xcom_push(key='etl_config', value=config)

# def get_monday_folder():
#     today=datetime.utcnow()
#     monday=today-timedelta(days=today.weekday())
#     return monday.strftime('%Y%m%d')

# def etl_transformation(**kwargs):
#     task_instance=kwargs['ti']
#     config=task_instance.xcom_pull(task_ids="get_yml_file_id",key='etl_config')
    
#     processing_week=kwargs.get('processing_week',get_monday_folder())

#     for rule in config['transform_rules']:
#         if 'add_dataframe' in rule:
#             add_df_rule= rule['add_dataframe']
#             break

#     raw_path=add_df_rule['file_path']
#     resolved_path=raw_path.replace('[processing_week]',processing_week)

#     bucket_name="weatherbucket-yml"

#     prefix=f"weather_data/input/raw_data/{processing_week}/"

#     s3 = S3Hook(aws_conn_id="weather_bucket_s3_conn")
#     all_keys = s3.list_keys(bucket_name=bucket_name, prefix=prefix)

#     csv_keys = []
#     file_hint = add_df_rule.get("file_name_hint", "")
#     for key in all_keys:
#         if file_hint in key and key.endswith('.csv'):
#             csv_keys.append(key)

#     df_list=[]
#     for key in csv_keys:
#         obj = s3.read_key(bucket_name=bucket_name, key=key)
#         df = pd.read_csv(StringIO(obj))
#         df_list.append(df)

#     df=pd.concat(df_list,ignore_index=True)


#     for rule in config['transform_rules']:
#         if 'drop_duplicate' in rule:
#             drop_config=rule['drop_duplicate']
            
#             subset=drop_config.get('subset',None)
#             keep=drop_config.get('keep',None)
#             df.drop_duplicates(subset=subset,keep=keep,inplace=True)

#         elif 'renaming_columns' in rule:
#             df.rename(columns=rule['renaming_columns']['rename_columns'], inplace= True)
        
#         elif 'country_mappings' in rule:
#             mapping=rule['country_mappings']['mapping']
#             col=rule['country_mappings']['column']

#             df[col]=df[col].map(mapping)

#         elif 'convert_temperature' in rule:
#             temp_config=rule['convert_temperature']
#             if temp_config['from_unit']=='kelvin' and temp_config['to_unit']=='celsius':
#                 for col in temp_config['columns']:
#                     if col in df.columns:
#                         df[col]=(df[col]-273.15).round(2)
#                     else:
#                         print(f"Warning: Column '{col}' not found in DataFrame.")
        
#         elif 'drop_columns' in rule:
#             col=rule['drop_columns']['columns']
#             df.drop(col, inplace=True, errors='ignore')
        
#         elif 'handle_missing' in rule:
#             method=rule['handle_missing']['method']
#             fill_val=rule['handle_missing'].get('fill_value',None)
#             if method=='drop':
#                 df.dropna(inplace=True)
#             elif method=='fill':
#                 df.fillna(value=fill_val,inplace=True)
                
#         elif 'reorder_columns' in rule:
#             col=rule['reorder_columns']['columns']
#             df = df[[c for c in col if c in df.columns]]

#     #storing in local path 
#     file_name=f"transformed_weather_data.csv"
#     local_path=os.path.join("/tmp",file_name)
#     df.to_csv(local_path, index=False)

#     task_instance.xcom_push(key='local_path', value=local_path)


# def store_transformed_data(**kwargs):

#     task_instance=kwargs['ti']

#     config=task_instance.xcom_pull(task_ids='get_yml_file_id',key='etl_config')

#     local_path=task_instance.xcom_pull(task_ids='etl_transformation_id',key='local_path')

#     for rule in config['transform_rules']:
#         if 'output_file' in rule:
#             output_config=rule['output_file']
#             break
    
#     processing_week=kwargs.get('processing_week',get_monday_folder())

#     output_folder_path=output_config['output_file_path']
    
#     output_key=output_folder_path.replace('[processing_week]',processing_week)

#     file_name=os.path.basename(local_path)
#     s3=S3Hook(aws_conn_id="weather_bucket_s3_conn")
#     s3.load_file(
#         filename=local_path,
#         key=f"{output_key}/{file_name}",
#         bucket_name="weatherbucket-yml",
#         replace=True
#     )
#     os.remove(local_path)


# # -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# default_args={
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'start_date': datetime(2025,7,9),
#     'retries': 2,
#     'retry_delay': timedelta(minutes=2)
#     }
# with DAG('weather_dag_etl',
#         default_args=default_args,
#         start_date=datetime(2025,7,9),
#         schedule_interval = "40 11 * * 0",
#         description="Fetch daily data for multiple cities and store it in an S3 bucket",
#         catchup=False
# ) as dag:
#     wait_for_acquisition_task=ExternalTaskSensor(
#         task_id='wait_for_data_acquisition',
#         external_dag_id='weather_dag_data_acquisition',
#         external_task_id='upload_to_s3',
#         execution_delta=timedelta(days=-7), 
#         mode='reschedule',
#         timeout=600,
#         poke_interval=60,
#         dag=dag
#     )
#     get_yml_file_task=PythonOperator(
#         task_id='get_yml_file_id',
#         python_callable= get_yml_file
#     )
#     etl_transformation_task=PythonOperator(
#         task_id='etl_transformation_id',
#         python_callable=etl_transformation,
#         provide_context=True
#     )
#     store_transformed_df_task = PythonOperator(
#     task_id='store_transformed_csv_id',
#     python_callable=store_transformed_data,
#     provide_context=True
#     )   
#     wait_for_acquisition_task >> get_yml_file_task >> etl_transformation_task>>store_transformed_df_task
    




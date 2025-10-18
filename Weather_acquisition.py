from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import get_current_context
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.email import EmailOperator
from datetime import datetime, timedelta, timezone
from airflow.decorators import dag, task
from airflow.models import Variable
import pandas as pdc
import requests
import json
import os
import logging
import traceback

DATASET_APIS = {
    "current_weather": lambda city, lat, lon, api_key: f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/{city}/today?unitGroup=metric&include=current&key={api_key}",
    "forecast_weather": lambda city, lat ,lon, api_key: f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/{city}?unitGroup=metric&include=current&key={api_key}",
    "hourly_forecast": lambda city, lat, lon, api_key: f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/{city}?unitGroup=metric&include=hours&key={api_key}",
    "pollution": lambda city, lat, lon, api_key: f"https://api.openweathermap.org/data/2.5/air_pollution?lat={lat}&lon={lon}&appid={api_key}",
    "uv_precipitation": lambda city, lat, lon, api_key: f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&hourly=rain,precipitation,uv_index&timezone=auto&forecast_days=1",
    "alerts": lambda city, lat, lon, api_key:f"https://api.weatherbit.io/v2.0/alerts?lat={lat}&lon={lon}&key=35e266b1fbb6447babd051d8eba97e21"
}

@dag(
    dag_id='weather_dag_data_acquisition',
    schedule_interval="0 0 * * *",
    start_date=datetime(2025, 7, 9),
    catchup=False,
    default_args={
        'owner': 'airflow',
        'retries': 0,
        'retry_delay': timedelta(minutes=2),
    },
    tags=["weather", "data", "daily"],
    default_view="graph",
    description="Fetch daily data for multiple cities and store in S3",
    doc_md="""
    ### 🌤️ Weather ETL DAG  
    This DAG collects weather data for major Asian cities using the OpenWeatherMap API.  
    It stores the raw data in `/tmp`, then uploads it to an S3 bucket for further analysis.  
    Features:
    - Reads city config from S3 (JSON)
    - Uses Airflow Variables for secure API key storage
    - Writes only small metadata via XCom (not large payloads)
    """
)
def weather_dag():

    @task()
    def get_city_json_file():
        bucket_name = "weatherbucket-yml"
        key = "weather_data/Automation/configuration/city/city.json"
        aws_conn_id = "weather_bucket_s3_conn"
        s3 = S3Hook(aws_conn_id=aws_conn_id)
        content = s3.read_key(bucket_name=bucket_name, key=key)
        cities = json.loads(content)
        if isinstance(cities, dict):
            return [city for group in cities.values() for city in group]
        return cities

    @task()
    def create_monday_folder():
        IST = timezone(timedelta(hours=5, minutes=30))
        today = datetime.now(IST)
        monday = today - timedelta(days=today.weekday()) #0-6 sunday=6
        return monday.strftime('%Y%m%d')

    @task()
    def fetching_data(cities):
        ctx = get_current_context()
        dataset = ctx["dag_run"].conf.get("dataset","all")
        api = Variable.get("openweather_api_key")
        api_2= Variable.get("visual_api_key")
        IST = timezone(timedelta(hours=5, minutes=30))
        today = datetime.now(IST).strftime("%Y%m%d")

        if dataset  not in DATASET_APIS and dataset!='all':
            raise ValueError(f"Invalid dataset parameter {dataset}")
        if dataset == "all":
            dataset_to_fetch=DATASET_APIS.keys()
        else:
            dataset_to_fetch=[dataset]

        local_files=[]
        for ds in dataset_to_fetch:
            ds_data={}
            for city_info in cities:
                city_name=city_info["city"]
                city_lat=city_info["latitude"]
                city_lon=city_info["longitude"]

                if ds=="current_weather" or ds=="hourly_forecast" or ds=="forecast_weather":
                    url=DATASET_APIS[ds](city=city_name, lat=city_lat, lon=city_lon, api_key=api_2)
                else:
                    url=DATASET_APIS[ds](city=city_name, lat=city_lat, lon=city_lon, api_key=api)

                try:
                    logging.info(f"🌍 Fetching dataset={ds}, city={city_name}, url={url}")
                    response=requests.get(url, timeout=30)
                    logging.info(f"✅ Response for {city_name} [{ds}] - Status: {response.status_code}")
                    response.raise_for_status()  # raises HTTPError if status != 200
                    ds_data[city_name]=response.json()
                except Exception as e:
                    logging.error(f"❌ Failed for city={city_name}, dataset={ds}, url={url}")
                    logging.error(traceback.format_exc())
                    ds_data[city_name]=None
                
            file_name=f"{ds}_data{today}.json"
            local_path=os.path.join("/tmp",file_name)

            with open(local_path, "w") as f:
                json.dump(ds_data,f)
            local_files.append(local_path)

        return local_files
     
    @task()
    def upload_to_s3(folder, local_files):
        try:
            s3 = S3Hook(aws_conn_id="weather_bucket_s3_conn")
            # today=datetime.utcnow().strftime('%Y%m%d')

            for file_path in local_files:
                    filename=os.path.basename(file_path)
                    dataset_type=filename.split("_")[0]
                    s3_key=f"weather_data/input/raw_data/{dataset_type}/{folder}/{filename}"
                    s3.load_file(
                        filename=file_path,
                        key=s3_key,
                        bucket_name="weatherbucket-yml",
                        replace=True
                    )
                    os.remove(file_path)
        except Exception as e:
            raise Exception(f"❌ Failed to upload to S3: {str(e)}")
        
    send_success_email = EmailOperator(
        task_id="send_success_email",
        to='nishantsingh27022004@gmail.com',   
        subject="✅ Weather DAG Data Acquisition Success",
        html_content="""
        <h3>DAG Run Completed Successfully!</h3>
        <p>All weather datasets have been processed and stored correctly.</p>
        """,
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    send_failure_email = EmailOperator(
    task_id='send_failure_email',
    to='nishantsingh27022004@gmail.com',
    subject='❌ Weather DAG Data Acquisition Failed!',
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

    cities = get_city_json_file()
    folder = create_monday_folder()
    local_files = fetching_data(cities)
    upload_to_s3( folder,local_files) >> [send_success_email, send_failure_email]

weather_dag()
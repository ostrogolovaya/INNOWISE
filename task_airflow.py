from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from datetime import datetime
import csv
import pandas as pd
import pymongo
from pymongo import MongoClient
#from airflow.providers.mongo.hooks.mongo import MongoHook
import json
import re

# def get_data_from_csv():
#     read_file = pd.read_csv("/opt/airflow/dags/tiktok_google_play_reviews.csv")
#     df = read_file.to_json
#     return df

def transform_data():
    #df = ti.xcom_pull(tasks_ids=['get_data'])
    df = pd.read_csv("/opt/airflow/dags/tiktok_google_play_reviews.csv")
    # df = pd.read_csv("tiktok_google_play_reviews.csv")
    df1 = df.dropna(axis=0, how='all')
    df2 = df1.replace( ('null','Null','NaN'),'-')
    df2['content'] = df2['content'].str.replace(r'[^A-Za-z0-9.,:!?-]',' ',regex = True)
    df2.sort_values(by=['at'], ascending=False).to_csv('/opt/airflow/dags/tiktok_google_play_reviews_after_changes.csv')
    # df2.sort_values(by=['at'], ascending=False).to_csv('tiktok_google_play_reviews_after_changes.csv')
# transform_data()

def push_data_to_mongodb():
    # hook = MongoHook(mongo_conn_id="mongodb+srv://ostroglovaya:Kate180896@cluster0.zyhampx.mongodb.net/?retryWrites=true&w=majority")
    # client = hook.get_conn()
    client = MongoClient("mongodb+srv://ostroglovaya:Kate180896@cluster0.zyhampx.mongodb.net/?retryWrites=true&w=majority")
    df = pd.read_csv("/opt/airflow/dags/tiktok_google_play_reviews_after_changes.csv")
    # df = pd.read_csv("tiktok_google_play_reviews_after_changes.csv")
    # data = df.to_dict('records')
    db = client['DB']
    collection = db['Tiktok_data']
    collection.insert_many(df.to_dict('records'))
# push_data_to_mongodb()

with DAG("get_data_from_csv", # Dag id
start_date=datetime(2022, 10 ,27), # start date
schedule='@daily',  # Cron expression, here it is a preset of Airflow.
catchup=False  # Catchup
) as dag:
    transform_data = PythonOperator(
    task_id = "transform_data",
    python_callable = transform_data
    )
    push_data = PythonOperator(
        task_id="push_data",
        python_callable=push_data_to_mongodb
    )

    transform_data>>push_data

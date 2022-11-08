import csv
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from datetime import datetime
import snowflake.connector
import csv
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine

engine = create_engine(
    'snowflake://{user}:{password}@{account_identifier}/'.format(
        user='ostrogolovaya',
        password='Kate180896_',
        account_identifier='RZ05090.eu-north-1.aws',
    )
)
connection = engine.connect()


def get_data_from_csv():
    df = pd.read_csv("/opt/airflow/dags/763K_plus_IOS_Apps_Info.csv", index_col=False)
    df.to_sql('raw_data_1', con=engine, index=False, if_exists='append', chunksize=1000)


def check_data_in_raw_table():
    connection.execute("MERGE INTO stage_data a USING raw_data_check b ON a._ID = b._ID WHEN MATCHED AND "
                       "metadata$action = 'DELETE' AND metadata$isupdate = 'FALSE' THEN DELETE WHEN MATCHED "
                       "AND metadata$action = 'INSERT' AND metadata$isupdate = 'TRUE' THEN UPDATE SET "
                       "a.\"IOS_App_Id\"=b.\"IOS_App_Id\", a.\"Title\"=b.\"Title\", a.\"Developer_Name\"=b.\"Developer_Name\","
                       "a.\"Developer_IOS_Id\"=b.\"Developer_IOS_Id\", a.\"IOS_Store_Url\"=b.\"IOS_Store_Url\","
                       "a.\"Seller_Official_Website\"=b.\"Seller_Official_Website\", a.\"Age_Rating\"=b.\"Age_Rating\","
                       "a.\"Total_Average_Rating\"=b.\"Total_Average_Rating\", a.\"Total_Number_of_Ratings\"=b.\"Total_Number_of_Ratings\","
                       "a.\"Average_Rating_For_Version\"=b.\"Average_Rating_For_Version\","
                       "a.\"Number_of_Ratings_For_Version\"=b.\"Number_of_Ratings_For_Version\", a.\"Original_Release_Date\"=b.\"Original_Release_Date\","
                       "a.\"Current_Version_Release_Date\"=b.\"Current_Version_Release_Date\", a.\"Price_USD\"=b.\"Price_USD\","
                       " a.\"Primary_Genre\"=b.\"Primary_Genre\", a.\"All_Genres\"=b.\"All_Genres\",a.\"Languages\"=b.\"Languages\","
                       "a.\"Description\"=b.\"Description\""
                       " WHEN NOT MATCHED AND metadata$action = 'INSERT' AND metadata$isupdate = 'FALSE' "
                       "THEN INSERT (_ID,\"IOS_App_Id\",\"Title\",\"Developer_Name\",\"Developer_IOS_Id\",\"IOS_Store_Url\","
                       "\"Seller_Official_Website\",\"Age_Rating\",\"Total_Average_Rating\",\"Total_Number_of_Ratings\","
                       "\"Average_Rating_For_Version\",\"Number_of_Ratings_For_Version\",\"Original_Release_Date\","
                       "\"Current_Version_Release_Date\",\"Price_USD\",\"Primary_Genre\",\"All_Genres\",\"Languages\",\"Description\") "
                       "VALUES (_ID,\"IOS_App_Id\",\"Title\",\"Developer_Name\",\"Developer_IOS_Id\",\"IOS_Store_Url\",\"Seller_Official_Website\","
                       "\"Age_Rating\",\"Total_Average_Rating\",\"Total_Number_of_Ratings\",\"Average_Rating_For_Version\",\"Number_of_Ratings_For_Version\","
                       "\"Original_Release_Date\",\"Current_Version_Release_Date\",\"Price_USD\",\"Primary_Genre\",\"All_Genres\",\"Languages\","
                       "\"Description\")")

def check_data_in_stage_table():
    connection.execute("MERGE INTO master_data a USING stage_data_check b ON a._ID = b._ID WHEN MATCHED AND "
                       "metadata$action = 'DELETE' AND metadata$isupdate = 'FALSE' THEN DELETE WHEN MATCHED "
                       "AND metadata$action = 'INSERT' AND metadata$isupdate = 'TRUE' THEN UPDATE SET "
                       "a.\"IOS_App_Id\"=b.\"IOS_App_Id\", a.\"Title\"=b.\"Title\", a.\"Developer_Name\"=b.\"Developer_Name\","
                       "a.\"Developer_IOS_Id\"=b.\"Developer_IOS_Id\", a.\"IOS_Store_Url\"=b.\"IOS_Store_Url\","
                       "a.\"Seller_Official_Website\"=b.\"Seller_Official_Website\", a.\"Age_Rating\"=b.\"Age_Rating\","
                       "a.\"Total_Average_Rating\"=b.\"Total_Average_Rating\", a.\"Total_Number_of_Ratings\"=b.\"Total_Number_of_Ratings\","
                       "a.\"Average_Rating_For_Version\"=b.\"Average_Rating_For_Version\","
                       "a.\"Number_of_Ratings_For_Version\"=b.\"Number_of_Ratings_For_Version\", a.\"Original_Release_Date\"=b.\"Original_Release_Date\","
                       "a.\"Current_Version_Release_Date\"=b.\"Current_Version_Release_Date\", a.\"Price_USD\"=b.\"Price_USD\","
                       " a.\"Primary_Genre\"=b.\"Primary_Genre\", a.\"All_Genres\"=b.\"All_Genres\",a.\"Languages\"=b.\"Languages\","
                       "a.\"Description\"=b.\"Description\""
                       " WHEN NOT MATCHED AND metadata$action = 'INSERT' AND metadata$isupdate = 'FALSE' "
                       "THEN INSERT (_ID,\"IOS_App_Id\",\"Title\",\"Developer_Name\",\"Developer_IOS_Id\",\"IOS_Store_Url\","
                       "\"Seller_Official_Website\",\"Age_Rating\",\"Total_Average_Rating\",\"Total_Number_of_Ratings\","
                       "\"Average_Rating_For_Version\",\"Number_of_Ratings_For_Version\",\"Original_Release_Date\","
                       "\"Current_Version_Release_Date\",\"Price_USD\",\"Primary_Genre\",\"All_Genres\",\"Languages\",\"Description\") "
                       "VALUES (_ID,\"IOS_App_Id\",\"Title\",\"Developer_Name\",\"Developer_IOS_Id\",\"IOS_Store_Url\",\"Seller_Official_Website\","
                       "\"Age_Rating\",\"Total_Average_Rating\",\"Total_Number_of_Ratings\",\"Average_Rating_For_Version\",\"Number_of_Ratings_For_Version\","
                       "\"Original_Release_Date\",\"Current_Version_Release_Date\",\"Price_USD\",\"Primary_Genre\",\"All_Genres\",\"Languages\","
                       "\"Description\")")

#connection.close()
#engine.dispose()

with DAG("snowflake_task",  # Dag id
         start_date=datetime(2022, 11, 6),  # start date
         schedule='@weekly',  # Cron expression, here it is a preset of Airflow.
         catchup=False  # Catchup
         ) as dag:
    get_data = PythonOperator(
        task_id="get_data",
        python_callable=get_data_from_csv
    )
    load_to_stage = PythonOperator(
        task_id="load_to_stage",
        python_callable=check_data_in_raw_table
    )
    load_to_master = PythonOperator(
        task_id="load_to_master",
        python_callable=check_data_in_stage_table
    )

    get_data>>load_to_stage>>load_to_master
from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import json


# Defining the DAG
with DAG(
    dag_id="nasa_apod_postgres",
    start_date=datetime(2026,10,3),
    schedule="@daily",
    catchup=False
) as dag:
    # Step 1: Create the table if it does not exist
    @task
    def create_table():
        # Initializing the PostgreSQL hook
        postgres_hook = PostgresHook(
            postgres_conn_id="postgres_connection"
        )
        # SQL query to create the table
        create_table_query = """
        CREATE TABLE IF NOT EXISTS apod_data (
            id SERIAL PRIMARY KEY,
            title VARCHAR(255),
            explanation TEXT,
            url TEXT,
            date DATE,
            media_type VARCHAR(50)
        );
        """
        # Execute the SQL query
        postgres_hook.run(create_table_query)

    ## step 2 : Extract the nasa api data(apod) - astronmy pictures of the day extact pipline
    
    extract_apod = HttpOperator(
    task_id='extract_apod',
    http_conn_id='nasa_api',
    endpoint='planetary/apod',
    method='GET',
    data={
        "api_key": "{{ conn.nasa_api.extra_dejson.api_key }}"
    },
    response_filter=lambda response: response.json(),
)

 ## step 3 : transform the data pick the information i nedd ti save 


    @task 
    def transform_apod_data(response):
        apod_data ={
        'title' : response.get('title' ,''),
        'explanation' : response.get('explanation' , ''),
        'url' : response.get('url', ''),
        'date' :response.get('date' ,'') ,
        'media_type' : response.get('media_type', '')
        }
        return apod_data

    ##  step 4 : Loading the data into the postgres
    
    @task
    def load_data_to_postgre(apod_data):
        ##Initizalize the postgreHook
        postgres_hook = PostgresHook(postgres_conn_id= 'postgres_connection')

        ##Define the Sql Insertt the   Querry 

        insert_query = """

        INSERT INTO apod_data (title , explanation, url, date, media_type)
        VALUES (%s,%s,%s,%s,%s);
        """

        ##Execute the SQL querry 

        postgres_hook.run(insert_query,parameters =  (
            apod_data['title'],
            apod_data['explanation'],
            apod_data['url'],
            apod_data['date'],
            apod_data['media_type']

        ))
    


    ## step 5 : Verify the data with dbviwer


    ##step 6 : Define the task dependencies 
    create_table () >> extract_apod 
    ##Ensure the tbale is created

    api_response = extract_apod.output

    ##transform 
    transformed_data = transform_apod_data(api_response)

    ##load
    load_data_to_postgre(transformed_data)


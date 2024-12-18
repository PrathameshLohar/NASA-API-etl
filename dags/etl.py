from airflow import DAG
from airflow.decorators import task
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago

import json

## define DAG

with DAG(
    dag_id="etl_pipeline",
    start_date=days_ago(1),
    schedule="@daily"
) as dag:
    
    ##Step1 : Create an table, if it doesn't exists.
    @task
    def create_table():
            ## Initializing the postgres hook
            postgres_hook= PostgresHook(postgres_conn_id= "my_postgres_connection")

            ## Write the SQL query
            create_table_query= """
            CREATE TABLE IF NOT EXISTS apod_data(
                id SERIAL PRIMARY KEY,
                title VARCHAR(255),
                explanation TEXT,
                url TEXT,
                date DATE,
                media_type VARCHAR(50)
            )"""

            ## Run the SQL query
            postgres_hook.run(create_table_query)

            
    ##Step2 : Extract the data from NASA API (APOD)- Astronomy Picture of the Day [Extract Pipeline].
    # https://api.nasa.gov/planetary/apod?api_key=7BbRvxo8uuzas9U3ho1RwHQQCkZIZtJojRIr293q
    extract_apod= SimpleHttpOperator(
          task_id= 'extract_apod',
          http_conn_id= 'nasa_api',## Connection ID Defined In Airflow For NASA API
          endpoint='planetary/apod', ## NASA API enpoint for APOD
          method='GET',
          data={"api_key":"{{ conn.nasa_api.extra_dejson.api_key}}"}, ## USe the API Key from the connection
          response_filter=lambda response:response.json(), ## Convert response to json
    )

    
    ##Step3 : Transform the data (Pick the neccessary data/ information)
    @task
    def transform_apod_data(response):
          apod_data={
            'title': response.get('title', ''),
            'explanation': response.get('explanation', ''),
            'url': response.get('url', ''),
            'date': response.get('date', ''),
            'media_type': response.get('media_type', '')
          }
          return apod_data
    

    ##Step4 : Load the data into Postgres SQL.

    @task
    def load_data_to_postgres(apod_data):
        ## Initialize the PostgresHook
        postgres_hook= PostgresHook(postgres_conn_id= "my_postgres_connection")
    
        ## Define the SQL Insert Query
        insert_query= """
        INSERT INTO apod_data (title, explanation, url, date, media_type)
        VALUES (%s, %s, %s, %s, %s);"""

        ## Execute the SQL Query
        postgres_hook.run(insert_query, parameters=(
            apod_data['title'],
            apod_data['explanation'],
            apod_data['url'],
            apod_data['date'],
            apod_data['media_type']
        ))



    ##Step5 : Verify the data - DBViewer.

    ## - Go to the https://dbeaver.io/download/ and install the dbeaver for your system environment.
    ## - Setup the new database connection, where password and user is postgres, as we mentioned in the docker-compose.yml
    ## - Test the connection.
    ## - Run the query SELECT * FROM apod_data;


    ##Step6 : Define the task dependencies.

    ## Extract
    create_table() >> extract_apod  ## Ensure the table is create befor extraction
    api_response=extract_apod.output

    ## Transform
    transformed_data=transform_apod_data(api_response)

    ## Load
    load_data_to_postgres(transformed_data)
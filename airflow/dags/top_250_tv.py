import airflow
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
import datetime
import os
import sys
from helper.scrape_imdb_charts import _get_soup, _scrape_movies , _load_to_bigQuery

# Creating an Environmental Variable for the service key configuration
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/opt/airflow/dags/configs/ServiceKey_GoogleCloud.json'

default_args = {
    'start_date': datetime.datetime.today(),
    'schedule_interval': '0 0 * * *' # Run every day at midnight    
}

with DAG(dag_id = 'top_250_tv', default_args = default_args, catchup=False) as dag:

    # Dag #1: Get the most popular movies
    @task
    def scrape_movies():
        soup = _get_soup(chart='top_250_tv')
        movie_df = _scrape_movies(soup)
        return movie_df
 
    # Dag #2: Load the most popular movies
    @task
    def load_movies(movies_df):
        _load_to_bigQuery(movies_df, chart='top_250_tv')



    # Dependencies
    movies_df = scrape_movies()
    load_movies(movies_df)
    




if __name__ == '__main__':
    #movie_names = scrape_most_popular_titles(soup)
    pass    



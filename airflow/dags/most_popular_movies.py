import airflow
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
import datetime
import os
import sys
from helper.scrape_imdb_charts import _get_soup, _scrape_movies, _process_movies,_getOrCreate_dataset,_getOrCreate_table , _load_to_bigQuery

# Creating an Environmental Variable for the service key configuration
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/opt/airflow/dags/configs/ServiceKey_GoogleCloud.json'

default_args = {
    'start_date': datetime.datetime.today(),
    'schedule_interval': '0 0 * * *' # Run every day at midnight    
}

with DAG(dag_id = 'most_popular_movies', default_args = default_args, catchup=False) as dag:

    # Dag #1: Get the most popular movies
    @task
    def scrape_most_popular_titles():
        soup = _get_soup(chart='most_popular_movies')
        movie_dict = _scrape_movies(soup)
        return movie_dict
    
    # Dag #2: Process the most popular movies
    @task
    def process_movies(movie_dict):
        movies_df = _process_movies(movie_dict)
        return movies_df
    
    # Dag #3: Load the most popular movies
    @task
    def load_movies(movies_df):
        _load_to_bigQuery(movies_df, chart='most_popular_movies')

    # Dependencies
    movie_dict = scrape_most_popular_titles()
    movies_df = process_movies(movie_dict)
    load_movies(movies_df)


if __name__ == '__main__':
    #movie_names = scrape_most_popular_titles(soup)
    pass    



# web_scraping helper
import requests
from bs4 import BeautifulSoup
import os
import sys
from google.cloud import bigquery
import datetime
import pandas as pd

# Creating an Environmental Variable for the service key configuration
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/opt/airflow/dags/configs/ServiceKey_GoogleCloud.json'

# Create a client
bigquery_client = bigquery.Client()

def _get_soup(url = 'https://www.imdb.com/chart/top/?ref_=nv_mv_250'):

    '''
    Get the BeautifulSoup object from a url.
    Args:
        - url(str) = url of the website
            Default: 'https://www.imdb.com/chart/top/?ref_=nv_mv_250'
    Returns:
        - soup(BeautifulSoup) = BeautifulSoup object
    '''
    
    # Send a get request and parse using BeautifulSoup
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    return soup


def _scrape_most_popular_titles(soup):

    '''
    Scrape the most popular titles from the IMDB website.
    Args:
        - soup(BeautifulSoup) = BeautifulSoup object
    Returns:
        - movie_names(list) = List of movie names
    '''

    # Find all movie names in the url
    movie_names = []
    titlesRefs = soup.find_all('td', {'class':'titleColumn'})

    # Collect movies into list
    for title in titlesRefs:
        movie_names.append(title.find("a").text)
    
    return movie_names

def _process_movies(movie_names):

    '''
    Process movie names list into a dataframe.
    Args:
        - movie_names(list) = List of movie names
    Returns:
        - movies_df(pandas.core.frame.DataFrame) = Dataframe of movie names
    '''

    # Create a dataframe
    movies_df = pd.DataFrame(movie_names, columns=['movie_name'])

    # Add index column
    movies_df['movie_id'] = movies_df.index

    # Add date column
    movies_df['date'] = datetime.datetime.today()

    # Reorder columns
    movies_df = movies_df[['movie_id', 'movie_name', 'date']]

    # Export to csv
    movies_df.to_csv('/opt/airflow/dags/most_popular_movies.csv', sep=',', index=False)

    return movies_df

# Create a dataset called test_dataset
def _getOrCreate_dataset(dataset_name :str, project_id = bigquery_client.project) -> bigquery.dataset.Dataset:

    '''
    Get dataset. If the dataset does not exists, create it.
    
    Args:
        - dataset_name(str) = Name of the new/existing data set.
        - project_id(str) = project id(default = The project id of the bigquery_client object)

    Returns:
        - dataset(google.cloud.bigquery.dataset.Dataset) = Google BigQuery Dataset
    '''

    print('Fetching Dataset...')

    try:
        # get and return dataset if exist
        dataset = bigquery_client.get_dataset(dataset_name)
        print('Done')
        print(dataset.self_link)
        return dataset

    except Exception as e:
        # If not, create and return dataset
        if e.code == 404:
            print('Dataset does not exist. Creating a new one.')
            bigquery_client.create_dataset(dataset_name)
            dataset = bigquery_client.get_dataset(dataset_name)
            print('Done')
            print(dataset.self_link)
            return dataset
        else:
            print(e)

def _getOrCreate_table(dataset_name:str, table_name:str) -> bigquery.table.Table:


    '''
    Create a table. If the table already exists, return it.
    
    Args:
        - table_name(str) = Name of the new/existing table.
        - dataset_name(str) = Name of the new/existing data set.
        - project_id(str) = project id(default = The project id of the bigquery_client object)

    Returns:
        - table(google.cloud.bigquery.table.Table) = Google BigQuery table
    '''

    # Grab prerequisites for creating a table
    dataset = _getOrCreate_dataset(dataset_name)
    project = dataset.project
    dataset = dataset.dataset_id
    table_id = project + '.' + dataset + '.' + table_name

    print('\nFetching Table...')

    try:
        # Get table if exists
        table = bigquery_client.get_table(table_id)
        print('Done')
        print(table.self_link)
    except Exception as e:

        # If not, create and get table
        if e.code == 404:
            print('Table does not exist. Creating a new one.')
            bigquery_client.create_table(table_id)
            table = bigquery_client.get_table(table_id)
            print(table.self_link)
        else:
            print(e)
    finally:
        return table

def _load_bigQuery_most_popular_movies(movie_names, dataset_name='imdb', project_name = 'imdb-388708', table_name = 'most_popular_movies', date_to_load = datetime.datetime.today()):

    '''
    Load data into BigQuery table.
    Args:
        - movie_names(list) = List of movie names
        - dataset_name(str) = Name of the new/existing data set.
        - project_id(str) = project id(default = The project id of the bigquery_client object)
        - table_name(str) = Name of the new/existing table.
        - date_to_load(datetime.datetime) = Date to load into the table
    Returns:
        - None
    Notes:
        - The function will create a new dataset and table if they do not exist.
        - The function will overwrite the table if it already exists.
    '''

    # Process data
    movies_df = _process_movies(movie_names)

    # Get table
    table = _getOrCreate_table(dataset_name=dataset_name, table_name=table_name)
    table_id = table.project + '.' + table.dataset_id + '.' + table.table_id

    print('\nLoading data into BigQuery...')
    # Load data into BigQuery
    # set primary key to movie_id
    # overwrite table if exists
    job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_TRUNCATE",
    schema=[
        bigquery.SchemaField("movie_id", "INTEGER", mode="REQUIRED", description="Primary Key"),
        bigquery.SchemaField("movie_name", "STRING"),
        bigquery.SchemaField("date", "TIMESTAMP"),
    
    ],
    field_delimiter = ',',
    source_format=bigquery.SourceFormat.CSV
                )

    load_job = bigquery_client.load_table_from_dataframe(
        movies_df, table_id, job_config=job_config
    ) 
    
    load_job.result() 
    print("Done. Loaded {} rows.".format(load_job.output_rows))

def main():
    soup = _get_soup()
    movie_names = _scrape_most_popular_titles(soup)
    #movies_df = _process_movies(movie_names)
    #_load_bigQuery_most_popular_movies(movie_names)
    print(movie_names)



if __name__ == '__main__':
    main()
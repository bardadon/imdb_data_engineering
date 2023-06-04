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

def _get_soup(chart):

    '''
    Get the BeautifulSoup object from a url.
    Args:
        - chart(str) = chart to scrape
            Options: 'most_popular_movies', 'top_250_movies', 'top_english_movies', 'top_250_tv'
    Returns:
        - soup(BeautifulSoup) = BeautifulSoup object
    '''
    
    # Send a get request and parse using BeautifulSoup
    if chart == 'most_popular_movies':
        url = 'https://www.imdb.com/chart/moviemeter?pf_rd_m=A2FGELUUNOQJNL&pf_rd_p=470df400-70d9-4f35-bb05-8646a1195842&pf_rd_r=5V6VAGPEK222QB9E0SZ8&pf_rd_s=right-4&pf_rd_t=15506&pf_rd_i=toptv&ref_=chttvtp_ql_2'
    
    if chart == 'top_250_movies':
        url = 'https://www.imdb.com/chart/top?pf_rd_m=A2FGELUUNOQJNL&pf_rd_p=470df400-70d9-4f35-bb05-8646a1195842&pf_rd_r=5V6VAGPEK222QB9E0SZ8&pf_rd_s=right-4&pf_rd_t=15506&pf_rd_i=toptv&ref_=chttvtp_ql_3'
    
    if chart == 'top_english_movies':
        url = 'https://www.imdb.com/chart/top-english-movies?pf_rd_m=A2FGELUUNOQJNL&pf_rd_p=470df400-70d9-4f35-bb05-8646a1195842&pf_rd_r=3YMHR1ECWH2NNG5TPH1C&pf_rd_s=right-4&pf_rd_t=15506&pf_rd_i=boxoffice&ref_=chtbo_ql_4'
    
    if chart == 'top_250_tv':
        url = 'https://www.imdb.com/chart/tvmeter?pf_rd_m=A2FGELUUNOQJNL&pf_rd_p=470df400-70d9-4f35-bb05-8646a1195842&pf_rd_r=J9H259QR55SJJ93K51B2&pf_rd_s=right-4&pf_rd_t=15506&pf_rd_i=topenglish&ref_=chttentp_ql_5'

    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    return soup


def _scrape_movies(soup):

    '''
    Scrape the most popular titles and ratings from the IMDB website.
    Args:
        - soup(BeautifulSoup) = BeautifulSoup object
    Returns:
        - movie_dict(dict) = Dictionary of movie names and ratings
    '''
    # Find all movie names in the url
    movie_names = []
    movie_years = []
    movie_ratings = []
    user_votings = []

    # Find all movie in the url
    titlesRefs = soup.find_all('td', {'class':'titleColumn'})
    ratingsRefs = soup.find_all('td', {'class':'ratingColumn imdbRating'})

    # Collect movie title, release year, ratings and user votings
    for title in titlesRefs:
        try:
            movie_names.append(title.find("a").text)
        except:
            print('Missing title. Replacing with -1')
            movie_names.append(-1)
        
        try:
            movie_years.append(int(title.find("span").text[1:-1]))
        except:
            print('Missing year. Replacing with -1')
            movie_years.append(-1)

    for rating in ratingsRefs:
        try:
            movie_ratings.append(float(rating.find("strong").text))    
        except:
            print('Missing rating. Replacing with -1')
            movie_ratings.append(-1)

        try:
            votes_str = rating.find("strong").attrs['title']
            votes_str = votes_str.split(' ')[3]
            votes_int = int(votes_str.replace(',', ''))
            user_votings.append(votes_int)
        except:
            user_votings.append(-1)

    # Create a dataframe
    movie_df = pd.DataFrame({'movie_name': movie_names, 'movie_year': movie_years, 'movie_rating': movie_ratings, 'user_votings': user_votings})

    # Add movie_id
    movie_df['movie_id'] = movie_df.index + 1

    # set date
    movie_df['update_date'] = datetime.datetime.today().strftime('%Y-%m-%d')

    # reorder columns
    movie_df = movie_df[['movie_id', 'movie_name', 'movie_year', 'movie_rating', 'user_votings' ,'update_date']]

    return movie_df

# Create a dataset called test_dataset
def _getOrCreate_dataset(dataset_name :str) -> bigquery.dataset.Dataset:

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

def _load_to_bigQuery(movie_names, chart, dataset_name='imdb'):

    '''
    Load data into BigQuery table.
    Args:
        - movie_names(list) = List of movie names
        - chart(str) = Name of the chart
            Options: most_popular_movies, top_250_movies, top_english_movies, top_250_tv
        - dataset_name(str) = Name of the new/existing data set.
        - project_id(str) = project id(default = The project id of the bigquery_client object)
        - date_to_load(datetime.datetime) = Date to load into the table
    Returns:
        - None
    Notes:
        - The function will create a new dataset and table if they do not exist.
        - The function will overwrite the table if it already exists.
    '''

    if chart == 'most_popular_movies':
       table_name = 'most_popular_movies'
    
    if chart == 'top_250_movies':
        table_name = 'top_250_movies'

    if chart == 'top_english_movies':
        table_name = 'top_english_movies'

    if chart == 'top_250_tv':
        table_name = 'top_250_tv'

    # Create a table
    table = _getOrCreate_table(dataset_name, table_name)

    # Create a job config
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        schema=[
            bigquery.SchemaField("movie_id", bigquery.enums.SqlTypeNames.INT64),
            bigquery.SchemaField("movie_name", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("movie_year", bigquery.enums.SqlTypeNames.INT64),
            bigquery.SchemaField("movie_rating", bigquery.enums.SqlTypeNames.FLOAT64),
            bigquery.SchemaField("user_votings", bigquery.enums.SqlTypeNames.INT64),
            bigquery.SchemaField("update_date", bigquery.enums.SqlTypeNames.DATE),
        ],
        write_disposition="WRITE_TRUNCATE",
    )

    # Load data into the table
    job = bigquery_client.load_table_from_dataframe(
        movie_names, table, job_config=job_config
    )

    # Wait for the job to complete
    job.result()

    # Check if the job is done
    print("Loaded {} rows into {}:{}.".format(job.output_rows, dataset_name, table_name))

def main():
    #soup = _get_soup(chart='top_250_movies')
    #movies_df = _scrape_movies(soup)
    #print(movies_df.head())
    #_load_to_bigQuery(movies_df, chart='top_250_movies')
    pass

if __name__ == '__main__':
    main()


    
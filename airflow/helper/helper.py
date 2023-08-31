import requests
import pandas as pd
import numpy as np

from google.cloud import bigquery

bigquery_client = bigquery.Client()

def _extract_covid_data():
    response = requests.get("https://api.covidtracking.com/v1/us/daily.csv")

    # export the data to a csv file
    with open('covid_data.csv', 'wb') as f:
        f.write(response.content)
        f.close()

def _pre_process():

    # Load data
    df = pd.read_csv('covid_data.csv')

    # Keep only records that include data from all 56 states
    df = df[df.states == 56]

    # Remove unnecessary columns
    df = df.drop(columns=["recovered", "lastModified", "states", "dateChecked", "total", "posNeg", "hospitalized"])

    # Reorder columns
    df = df[["hash", "date", "positive", "negative", "positiveIncrease", "negativeIncrease", "pending", "hospitalizedCurrently", "hospitalizedIncrease", "hospitalizedCumulative",
             "inIcuCurrently", "inIcuCumulative", "onVentilatorCurrently", "onVentilatorCumulative", "totalTestResults", "totalTestResultsIncrease",
             "death", "deathIncrease" 
             ]]
    
    # Rename columns
    df.rename(columns={
        "negative": "pcr_test_negative",
        "positive": "pcr_test_positive"
    }, inplace=True)

    # Convert date to datetime
    df["date"] = df.date.astype('str')
    df["date"] = pd.to_datetime(df.date.str[0:4]+'-'+df.date.str[4:6]+'-'+df.date.str[6:8])

    # Export to csv
    df.to_csv("clean_covid_data.csv")
    
    return df


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


def _load_to_bigQuery(dataset_name='covid_analysis', table_name='covid_data'):

    '''
    Load CSV file to BigQuery.

    Args:
    - date_to_load(String)
    - Default - today
    Returns:
    - None
    '''

    # Get table
    table = _getOrCreate_table(dataset_name=dataset_name, table_name=table_name)

    # Load CSV file to BigQuery
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("hash", "STRING"),
            bigquery.SchemaField("date", "DATE"),
            bigquery.SchemaField("pcr_test_positive", "INTEGER"),
            bigquery.SchemaField("pcr_test_negative", "INTEGER"),
            bigquery.SchemaField("positiveIncrease", "INTEGER"),
            bigquery.SchemaField("negativeIncrease", "INTEGER"),
            bigquery.SchemaField("pending", "INTEGER"),
            bigquery.SchemaField("hospitalizedCurrently", "INTEGER"),
            bigquery.SchemaField("hospitalizedIncrease", "INTEGER"),
            bigquery.SchemaField("hospitalizedCumulative", "INTEGER"),
            bigquery.SchemaField("inIcuCurrently", "INTEGER"),
            bigquery.SchemaField("inIcuCumulative", "INTEGER"),
            bigquery.SchemaField("onVentilatorCurrently", "INTEGER"),
            bigquery.SchemaField("onVentilatorCumulative", "INTEGER"),
            bigquery.SchemaField("totalTestResults", "INTEGER"),
            bigquery.SchemaField("totalTestResultsIncrease", "INTEGER"),
            bigquery.SchemaField("death", "INTEGER"),
            bigquery.SchemaField("deathIncrease", "INTEGER")
        ],
        skip_leading_rows=1,
        source_format=bigquery.SourceFormat.CSV,
    )

    data_to_load = pd.read_csv('clean_covid_data.csv', index_col=0)
    data_to_load = data_to_load.replace(np.nan, 0)

    job = bigquery_client.load_table_from_dataframe(
        data_to_load, table, job_config=job_config
    )  # Make an API request.

    job.result()  # Wait for the job to complete.

    print("Loaded {} rows into {}:{}.".format(job.output_rows, dataset_name, table_name))


if __name__ == "__main__":
    _extract_covid_data()
    df = _pre_process()

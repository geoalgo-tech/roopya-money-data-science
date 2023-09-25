import functions_framework
import json
import string
import random
from google.cloud import bigquery
import pandas as pd
from google.cloud import storage


def token_gen():
    token=''
    for i in range(0,12):
        char_i=random.choice(string.ascii_letters)
        token=token+char_i
    return token
token=token_gen()
print(token)


# Replace with your BigQuery project ID, dataset ID, and location
PROJECT_ID = 'geoalgo-208508'
DATASET_ID = 'roopya_analytics_dw'
LOCATION = 'asia-south1'  # Replace with your desired location (e.g., 'US' or 'EU')
bucket_name = 'roopya_analytics_workarea'
base_filename = 'PYTHON_PREPROCESS1'
#token = random.choices(string.ascii_letters, k=12)
object_path = f'Swarnavo/Pipeline/{base_filename}_{token}.csv'

# Define the schema for the BigQuery table
SCHEMA = [
    bigquery.SchemaField('name1', 'STRING', mode='REQUIRED'),
    bigquery.SchemaField('name2', 'STRING', mode='REQUIRED'),
]

SCHEMA1 = [
    bigquery.SchemaField('name1', 'STRING', mode='REQUIRED'),
    bigquery.SchemaField('name2', 'STRING', mode='REQUIRED'),
]

SCHEMA2 = [
    bigquery.SchemaField('name1', 'STRING', mode='REQUIRED'),
    bigquery.SchemaField('name2', 'STRING', mode='REQUIRED'),
    bigquery.SchemaField('full_name', 'STRING', mode='REQUIRED'),
]

# Initialize the BigQuery client with the specified location
client = bigquery.Client(project=PROJECT_ID, location=LOCATION)
storage_client = storage.Client(project=PROJECT_ID)


def insert_rows_into_bigquery(table_ref, rows_to_insert):
    try:
        # Explicitly specify the schema when inserting rows
        errors = client.insert_rows(table_ref, rows_to_insert, selected_fields=SCHEMA)
        if not errors:
            return "Data inserted successfully."
        else:
            return f"Error inserting data: {errors}"
    except Exception as e:
        return f"Error inserting data: {str(e)}"


def PROD_TABLE(table_id, table_ref, token):
    # 1. Run a SQL query as SELECT name1, name2 FROM 'table_id'
    query = f"SELECT name1, name2 FROM `{PROJECT_ID}.{DATASET_ID}.{table_id}`"

    # 2. Create an empty table in BigQuery with SCHEMA1. Name it as "preprocess1"
    preprocess_table_id = 'PREPROCESS1_' + ''.join(token)

    # Create a reference to the preprocess1 table
    preprocess_table_ref = client.dataset(DATASET_ID).table(preprocess_table_id)

    # Create the preprocess1 table with the defined schema (SCHEMA1)
    preprocess_table = bigquery.Table(preprocess_table_ref, schema=SCHEMA1)
    preprocess_table = client.create_table(preprocess_table)

    # 3. Save the results of the SQL query in preprocess1
    job_config = bigquery.QueryJobConfig(destination=preprocess_table_ref)
    query_job = client.query(query, job_config=job_config)
    query_job.result()  # Wait for the query to finish

    # Delete the old table
    client.delete_table(table_ref)
    print("----------------------------------------------------------------")
    print("Successfully deleted: ", table_id)
    print("----------------------------------------------------------------")
    print("Data inserted successfully into: ", preprocess_table_id)
    return


def PREPROCESS1_TABLE(table_id, table_ref, token):
    # 1. Run a SQL query as SELECT name1, name2 FROM 'table_id'
    query = f"SELECT name1, name2, CONCAT(name1, ' ', name2) AS full_name FROM `{PROJECT_ID}.{DATASET_ID}.{table_id}`"

    # 2. Create an empty table in BigQuery with SCHEMA2. Name it as "preprocess2"
    preprocess_table_id = 'PREPROCESS2_' + ''.join(token)

    # Create a reference to the preprocess2 table
    preprocess_table_ref = client.dataset(DATASET_ID).table(preprocess_table_id)

    # Create the preprocess2 table with the defined schema (SCHEMA2)
    preprocess_table = bigquery.Table(preprocess_table_ref, schema=SCHEMA2)
    preprocess_table = client.create_table(preprocess_table)

    # 3. Save the results of the SQL query in preprocess2
    job_config = bigquery.QueryJobConfig(destination=preprocess_table_ref)
    query_job = client.query(query, job_config=job_config)
    query_job.result()  # Wait for the query to finish

    # Delete the old table
    client.delete_table(table_ref)
    print("----------------------------------------------------------------")
    print("Successfully deleted: ", table_id)
    print("----------------------------------------------------------------")
    print("Data inserted successfully into: ", preprocess_table_id)
    return


def Python1(table_id, token):
    # 1. Run a SQL query as SELECT name1, name2 FROM 'table_id'
    query = f"SELECT name1, name2, full_name FROM `{PROJECT_ID}.{DATASET_ID}.{table_id}`;"
    query_job = client.query(query)
    results = query_job.result()

    # Convert the query results into a DataFrame
    df = results.to_dataframe()
    df['new_name'] = df['name1'] + df['name2']  # Replace 'New Value' with your desired value
    print(df.shape)

    #base_filename = 'PYTHON_PREPROCESS1'


    csv_data = df.to_csv(index=False)

    # Get the GCS bucket
    bucket = storage_client.get_bucket(bucket_name)

    # Create a GCS Blob and upload the CSV data
    blob = bucket.blob(object_path)
    blob.upload_from_string(csv_data, content_type='text/csv')

    print(f'DataFrame saved to GCS: gs://{bucket_name}/{object_path}')

    return


@functions_framework.http
def save_to_bigquery(request):
    request_json = request.get_json(silent=True)

    if request_json and isinstance(request_json, list):
        # Generate a random token for the new table
        #token = random.choices(string.ascii_letters, k=12)
        table_id = 'PROD_' + ''.join(token)

        # Create a reference to the BigQuery table with the random token
        table_ref = client.dataset(DATASET_ID).table(table_id)

        # Create the table with the defined schema if it doesn't exist
        try:
            table = bigquery.Table(table_ref, schema=SCHEMA)
            table = client.create_table(table)
        except Exception as e:
            pass

        # Prepare the rows to insert into the table
        rows_to_insert = []
        for item in request_json:
            if 'name1' in item and 'name2' in item:
                name1 = item['name1']
                name2 = item['name2']
                row = {
                    'name1': name1,
                    'name2': name2,
                }
                rows_to_insert.append(row)

        # Insert the rows into the BigQuery table with the defined schema
        if rows_to_insert:
            result = insert_rows_into_bigquery(table_ref, rows_to_insert)
            print("Table Name: ", table_id)
            PROD_TABLE(table_id, table_ref, token)
            preprocess_table_id = 'PREPROCESS1_' + ''.join(token)
            preprocess_table_ref = client.dataset(DATASET_ID).table(preprocess_table_id)
            PREPROCESS1_TABLE(preprocess_table_id, preprocess_table_ref, token)
            preprocess_table_id_2 = 'PREPROCESS2_' + ''.join(token)
            preprocess_table_ref_2 = client.dataset(DATASET_ID).table(preprocess_table_id_2)
            # Python function preprocess1
            Python1(preprocess_table_id_2, token)
            preprocess_table_id_3 = 'PYTHON_PREPROCESS1_' + ''.join(token)
            client.delete_table(preprocess_table_ref_2)
            print("----------------------------------------------------------------")
            print("Successfully deleted: ", preprocess_table_id_2)

            print("----------------------------------------------------------------")
            return f"All BigQuery steps are successful, Table deleated as: '{preprocess_table_id_2}' and Preprocessed python DataFrame saved to GCS: gs://{bucket_name}/{object_path}."

        else:
            return "No valid data to insert."
    else:
        return "Invalid input format or no data to process."
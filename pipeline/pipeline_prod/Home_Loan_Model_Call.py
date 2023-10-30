import functions_framework
import json
import string
import random
import re
import io
from google.cloud import bigquery
import pandas as pd
from google.cloud import storage
import numpy as np
from scipy.stats.mstats import winsorize
from sklearn.preprocessing import StandardScaler
import seaborn as sns
import matplotlib.pyplot as plt
from sklearn import preprocessing
import requests
import google.auth
import google.auth.transport.requests

PROJECT_ID = 'geoalgo-208508'
DATASET_ID = 'roopya_analytics_dw'
LOCATION = 'asia-south1'  # Replace with your desired location (e.g., 'US' or 'EU')
bucket_name = 'roopya_analytics_workarea'

# Initialize the BigQuery client with the specified location
client = bigquery.Client(project=PROJECT_ID, location=LOCATION)
storage_client = storage.Client(project=PROJECT_ID)



def Home_Loan_Model_Call(df_request_json, token):

    df = df_request_json.copy()

    dh = df[df['ACCOUNT_TYPE_Home Loan'] != 0]

    pd.options.mode.chained_assignment = None

    dh['AGE_COHORT'].fillna(dh['AGE_COHORT'].mean(), inplace=True)

    def convert_to_months(age_str):
        years, months = age_str.split(' ')
        total_months = int(years.strip('yrs')) * 12 + int(months.strip('mon'))
        return total_months

    dh['AVERAGE_ACCOUNT_AGE'] = dh['AVERAGE_ACCOUNT_AGE'].apply(convert_to_months)

    dh['ROOPYA_CUSTOMER_STATUS'] = dh['ROOPYA_CUSTOMER_STATUS'].map({'Good': 1, 'Bad': 0})

    WOE_dictionary = {
    'MH': -0.0415749986799236,
    'GJ': -0.2235031497688723,
    'WB': 0.2653150732625036,
    'MP': 0.5279443172982753,
    'DL': 0.10395221601077305,
    'AP': -0.12429389809857178,
    'KA': -0.17172343578496133,
    'CG': 0.6171177248656481,
    'PB': 0.254228916136814,
    'UP': -0.049344816103512115,
    'CH': -0.5735800293391802,
    'HR': -0.3569758233039565,
    'TN': 0.13989460862033512,
    'KL': 0.2389299660366667,
    'RJ': 0.24910008388707733,
    'UK': 0.29293131332384853,
    'BR': 0.0977083574082658,
    'JH': 0.16798905907964595,
    'HP': 0.25125861692328033,
    'GA': -0.15886416145886503,
    'OR': 0.12510733159638007,
    'AS': 0.4008946163960121,
    'ML': 0.4252119240467184,
    'JK': 1.3526759565189987,
    'PY': 0.24289036725276353,
    'DN': -0.32620416463720275,
    'AN': 2.197168765978594,
    'DD': 0.8256894906438438,
    'MN': 1.9858596723113868,
    'SK': 1.8249293054987494,
    'AR': 2.3223319089325996,
    'TR': 1.1802345083247512,
    'NL': 1.939339656676494,
    'MZ': 3.2386226408067547,
    'LD': 3.644087748914919
    }
    dh['STATE'] = dh['STATE'].map(WOE_dictionary)

    X = dh.drop(columns=['CREDIT_REPORT_ID', 'ROOPYA_CUSTOMER_STATUS', 'CCC', 'COP', 'FRB', 'HFC', 'NAB',	'Delinquent', 'Written Off',  'Delinquent_CURRENT_BALANCE',
       'Written Off_CURRENT_BALANCE', 'Delinquent_OVERDUE_AMOUNT', 'Written Off_OVERDUE_AMOUNT'])
    X.fillna(0, inplace=True)
    # df12 = X

    # base_filename_X = 'HOME_LOAN_Independent variable data'
    # object_path_X = f'Swarnavo/Pipeline/{base_filename_X}_{token}.csv'
    # csv_data_df12 = df12.to_csv(index=False)
    # bucket = storage_client.get_bucket(bucket_name)

    # blob = bucket.blob(object_path_X)
    # blob.upload_from_string(csv_data_df12, content_type='text/csv')

    # print(f'HOME_LOAN_Independent variable data DataFrame saved to GCS: gs://{base_filename_X}/{object_path_X}')

    # print('HOME_LOAN_Independent variable data uploaded sucessfully')

    # print("Model level Data Preprocessing successful")

    def call_endpoint(X):

        payload = X.to_json(orient='records')

        # base_filename_json = 'Payload'
        # object_path_json = f'Swarnavo/Pipeline/{base_filename_json}_{token}.json'
        # json_data = X.to_json(orient='records') 
        # bucket = storage_client.get_bucket(bucket_name)

        # blob = bucket.blob(object_path_json)
        # blob.upload_from_string(json_data, content_type='application/json')

        dic = {}
        vl = []
        values_list = []


        dic["instances"] = X.values.tolist()
        json_request = json.dumps(dic)
        print(json_request)
        #data = X.values.tolist()
        #print(data)

        #print(result)
        #print(instances_data)
        print('*******************************************')

        creds, project = google.auth.default()

        # creds.valid is False, and creds.token is None
        # Need to refresh credentials to populate those

        auth_req = google.auth.transport.requests.Request()
        creds.refresh(auth_req)
        
        print(creds.token)


        #print('Payload:',payload)
        API_URL = 'https://asia-south1-aiplatform.googleapis.com/v1/projects/303650502197/locations/asia-south1/endpoints/909771105193951232:predict'
        headers = {'Content-Type': 'application/json', 'Authorization': 'Bearer ' + creds.token}

        try:
            response = requests.post(API_URL, data=json_request, headers=headers)
            
            # Check for a successful response (HTTP status code 200)
            if response.status_code == 200:
                print("Request was successful")
                print(response.json())  # Print the API response
                prediction_data = response.json()
                base_filename_prediction = 'HOMELOAN_Prediction'
                object_path_prediction = f'Swarnavo/Pipeline/{base_filename_prediction}_{token}.json'
                json_data = response.json() 
                bucket = storage_client.get_bucket(bucket_name)

                blob = bucket.blob(object_path_prediction)
                blob.upload_from_string(json.dumps(prediction_data), content_type='application/json')

                print('Prediction uploaded sucessfully')
                print('Endpoint hitted succesfully')

                prediction_str = f"The prediction is: {json.dumps(prediction_data)}"

                return prediction_str
            else:
                print(f"Request failed with status code: {response.status_code}")
                print(response.text)  # Print the response content for debugging
        except Exception as e:
            print(f"An error occurred: {str(e)}")

            return

    call_endpoint(X)

    return
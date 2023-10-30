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



def Personal_Loan_Model_Call(df_request_json, token):
    df = df_request_json.copy()
    df = df[df['ACCOUNT_TYPE_Personal Loan'] != 0]
    print(df)
    def convert_to_months(value):
        parts = value.split()
        years = 0
        months = 0
        for part in parts:
            if 'yr' in part:
                try:
                    years = int(part.replace('yrs', ''))
                except ValueError:
                    pass  # Handle invalid year values (e.g., '2s') gracefully
            elif 'mon' in part:
                try:
                    months = int(part.replace('mon', ''))
                except ValueError:
                    pass  # Handle invalid month values (e.g., '10x') gracefully
        total_months = years * 12 + months
        return total_months
    
    # Apply the function to the DataFrame
    df['AVERAGE_ACCOUNT_AGE_IN_MONTHS'] = df['AVERAGE_ACCOUNT_AGE'].apply(convert_to_months)

    df.drop('AVERAGE_ACCOUNT_AGE', axis=1, inplace=True)

    # Drop unwanted columns
    df = df.drop(columns=['CREDIT_REPORT_ID','Active_CURRENT_BALANCE','Active_OVERDUE_AMOUNT', 'INCOME','OVERDUE_AMOUNT'])
    print(df['ACCOUNT_TYPE_Personal Loan'])
    median_age_cohort = df['AGE_COHORT'].median()
    df['AGE_COHORT'].fillna(median_age_cohort, inplace=True)
    df.drop('PRIMARY_DISTRIBUTED_AMOUNT', axis=1, inplace=True)
    df['ROOPYA_CUSTOMER_STATUS'] = df['ROOPYA_CUSTOMER_STATUS'].map({'Good': 0, 'Bad': 1})
    print(df)
    age_cohort_woe_dict = {
            40: -0.07651881196856762, 35: 0.40170267743693555, 50: -0.27868446742193087, 30: 0.40004198584695727, 45: -0.16962531863576105, 55: -0.28193874352780546, 60: -0.3227346191486958, 25: 0.39704554167170997, 20: 2.9230872273957416
    }
    df['AGE_COHORT'] = df['AGE_COHORT'].map(age_cohort_woe_dict)

    state_woe_dict = {
            'UP ': 0.05829914460917811, 'HR ': -0.05103419350710945, 'DL ': 0.018784504479276126, 'KA ': -0.18295752373556481, 'MH ': -0.05482933135644609, 'UK ': 0.09024507859248845, 'MP ': 0.07336810166867788, 'PB ': 0.03780732962554705, 
            'GA ': 0.0037187707543710124, 'WB ': 0.007452220928553237, 'AP ': 0.08961713843557143, 'TN ': 0.04453157580321886, 
            'JH ': 0.03603900573027656, 'CH ': -0.05189358176171171, 'RJ ': 0.07248312053034223, 'GJ ': -0.05845352372997492, 
            'KL ': -0.13244183846027835, 'CG ': 0.14486039498242423, 'TR ': -0.002990580204559085, 'OR ': 0.22099753980295225, 
            'JK ': 0.6636860335145786, 'AS ': 0.21662965503583395, 'BR ': 0.19989597527162475, 'HP ': 0.20464795000484237, 
            'ML ': -0.05834576614683189, 'MN ': 0.704919476214279, 'AN ': 0.13899089830448055, 'PY ': -0.09353640518962572, 
            'AR ': 0.22723229938735004, 'DD ': -0.39932927641329946, 'DN ': -0.5186896951547378, 'NL ': -0.7115706725515557, 
            'SK ': 0.03129198171472317, 'MZ ': 1.0047650348592574, 'LD ': 1.0772605368974106
        }

    df['STATE'] = df['STATE'].map(state_woe_dict)
    print(df['ROOPYA_CUSTOMER_STATUS'])
    print(df.columns)
    

    X = df.drop(columns=['ROOPYA_CUSTOMER_STATUS', 'CCC', 'COP', 'FRB', 'HFC', 'NAB',	'Delinquent', 'Written Off',  'Delinquent_CURRENT_BALANCE',
       'Written Off_CURRENT_BALANCE', 'Delinquent_OVERDUE_AMOUNT', 'Written Off_OVERDUE_AMOUNT'])
    X.fillna(0, inplace=True)
    # df12 = X

    # base_filename_X = 'PERSONAL_LOAN_Independent variable data'
    # object_path_X = f'Swarnavo/Pipeline/{base_filename_X}_{token}.csv'
    # csv_data_df12 = df12.to_csv(index=False)
    # bucket = storage_client.get_bucket(bucket_name)

    # blob = bucket.blob(object_path_X)
    # blob.upload_from_string(csv_data_df12, content_type='text/csv')

    # print(f'PERSONAL_LOAN_Independent variable data DataFrame saved to GCS: gs://{base_filename_X}/{object_path_X}')

    # print('PERSONAL_LOAN_Independent variable data uploaded sucessfully')

    # print("Model level Data Preprocessing successful")

    def call_endpoint(X):

        payload = X.to_json(orient='records')


        # base_filename_json = 'PersonalLoan_Payload'
        # object_path_json = f'Swarnavo/Pipeline/{base_filename_json}_{token}.json'
        # json_data = X.to_json(orient='records') 
        # bucket = storage_client.get_bucket(bucket_name)

        # blob = bucket.blob(object_path_json)
        # blob.upload_from_string(json_data, content_type='application/json')

        # print('Payload uploaded sucessfully')


        dic = {}
        vl = []
        values_list = []


        dic["instances"] = X.values.tolist()
        json_request = json.dumps(dic)
        print(json_request)

        creds, project = google.auth.default()

        # creds.valid is False, and creds.token is None
        # Need to refresh credentials to populate those

        auth_req = google.auth.transport.requests.Request()
        creds.refresh(auth_req)
        
        print(creds.token)


        #print('Payload:',payload)
        API_URL = 'https://asia-south1-aiplatform.googleapis.com/v1/projects/303650502197/locations/asia-south1/endpoints/6834573074348638208:predict'
        headers = {'Content-Type': 'application/json', 'Authorization': 'Bearer ' + creds.token}

        try:
            response = requests.post(API_URL, data=json_request, headers=headers)
            
            # Check for a successful response (HTTP status code 200)
            if response.status_code == 200:
                print("Request was successful")
                print(response.json())  # Print the API response
                prediction_data = response.json()
                base_filename_prediction = 'PERSONALLOAN_Prediction'
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
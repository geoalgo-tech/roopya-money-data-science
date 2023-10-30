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
from flask import request, Response
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



def Auto_Loan_Model_Call(df_request_json, token):
        df = df_request_json.copy()
        df = df[df['ACCOUNT_TYPE_Auto Loan'] != 0]
        df = df.reset_index(drop=True)
        #To avoid warnings
        pd.options.mode.chained_assignment = None

        def convert_to_months(row):
            parts = row.split()
            years = int(parts[0].replace('yrs', ''))
            months = int(parts[1].replace('mon', ''))
            return years * 12 + months
        # Apply the conversion function to the AVERAGE_ACCOUNT_AGE column
        df['AVERAGE_ACCOUNT_AGE'] = df['AVERAGE_ACCOUNT_AGE'].apply(convert_to_months)


        df['ROOPYA_CUSTOMER_STATUS'] = df['ROOPYA_CUSTOMER_STATUS'].replace({'Bad': 0, 'Good': 1})

        # Calculate the median of the column
        median_value = df['AGE_COHORT'].median()

        # Replace null values with the median value
        df['AGE_COHORT'].fillna(median_value, inplace=True)

        scaled_df = df.copy()
        df1 = scaled_df.copy()
        df1 = df1.drop(columns = ['PRIMARY_SANCTIONED_AMOUNT'])

        age_cohort_woe_dict = {
            "35": -0.3905364804545106,
            "30": -0.36219831191514384,
            "25": -0.2171104131074959,
            "45": 0.12074231523923198,
            "20": 0.14411881816993755,
            "40": 0.16749532110064314,
            "55": 0.27469090614210345,
            "50": 0.2803672614742691,
            "60": 0.3418813664167584
        }

        df1['AGE_COHORT'] = df1['AGE_COHORT'].map(age_cohort_woe_dict)

        state_woe_dict = {
            "MZ ": -1.1668437519521144,
            "AN ": -0.8278683851127832,
            "JK ": -0.6796586628256918,
            "MN ": -0.6300426417828636,
            "HP ": -0.5559346696291413,
            "BR ": -0.48388474729235936,
            "CG ": -0.461775900429858,
            "OR ": -0.4463234607222707,
            "TR ": -0.40260064970843906,
            "AR ": -0.35786475586704786,
            "PY ": -0.2923501487564197,
            "JH ": -0.20486681391959594,
            "AS ": -0.14099081756643347,
            "MP ": -0.12060561364555582,
            "AP ": -0.11294232887402875,
            "WB ": -0.1084064092055317,
            "UP ": -0.10172485086397329,
            "NL ": -0.10172485086397329,
            "UK ": -0.0867619502774938,
            "TN ": -0.08466238034095747,
            "RJ ": -0.07166546322458317,
            "PB ": -0.004058506032794042,
            "GA ": 0.05825764671510376,
            "MH ": 0.07837201090787037,
            "GJ ": 0.14960922081437553,
            "KA ": 0.15818002637915088,
            "HR ": 0.17656804948539692,
            "DL ": 0.24387293322349538,
            "KL ": 0.24516172015641127,
            "SK ": 0.24542609557103615,
            "CH ": 0.29543651614569694,
            "DD ": 0.38852693921171083,
            "DN ": 1.4570617726425428,
            "ML ": 1.4722135776631413
        }

        df1['STATE'] = df1['STATE'].map(state_woe_dict)

        X = df1.drop(columns=['CREDIT_REPORT_ID', 'ROOPYA_CUSTOMER_STATUS', 'CCC', 'COP', 'FRB', 'HFC', 'NAB',	'Delinquent', 'Written Off',  'Delinquent_CURRENT_BALANCE',
       'Written Off_CURRENT_BALANCE', 'Delinquent_OVERDUE_AMOUNT', 'Written Off_OVERDUE_AMOUNT'])
        X.fillna(0, inplace=True)
        # df12 = X

        # base_filename_X = 'AUTO_LOAN_Independent variable data'
        # object_path_X = f'Swarnavo/Pipeline/{base_filename_X}_{token}.csv'
        # csv_data_df12 = df12.to_csv(index=False)
        # bucket = storage_client.get_bucket(bucket_name)

        # blob = bucket.blob(object_path_X)
        # blob.upload_from_string(csv_data_df12, content_type='text/csv')

        # print(f'AUTO_LOAN_Independent variable data DataFrame saved to GCS: gs://{base_filename_X}/{object_path_X}')

        # print('AUTO_LOAN_Independent variable data uploaded sucessfully')

        # print("Model level Data Preprocessing successful")

        def call_endpoint(X):

            payload = X.to_json(orient='records')

            # base_filename_json = 'AutoLoan_Payload'
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
            API_URL = 'https://asia-south1-aiplatform.googleapis.com/v1/projects/303650502197/locations/asia-south1/endpoints/3288692055236149248:predict'
            headers = {'Content-Type': 'application/json', 'Authorization': 'Bearer ' + creds.token}

            try:
                response = requests.post(API_URL, data=json_request, headers=headers)
                
                # Check for a successful response (HTTP status code 200)
                if response.status_code == 200:
                    print("Request was successful")
                    print(response.json())  # Print the API response
                    prediction_data = response.json()
                    base_filename_prediction = 'AUTOLOAN_Prediction'
                    object_path_prediction = f'Swarnavo/Pipeline/{base_filename_prediction}_{token}.json'
                    json_data = response.json() 
                    bucket = storage_client.get_bucket(bucket_name)

                    blob = bucket.blob(object_path_prediction)
                    blob.upload_from_string(json.dumps(prediction_data), content_type='application/json')

                    print('Prediction uploaded sucessfully')
                    print('Endpoint hitted succesfully')

                    prediction_str = f"The prediction is: '{json.dumps(prediction_data)}'"

                    return prediction_str
                else:
                    print(f"Request failed with status code: {response.status_code}")
                    print(response.text)  # Print the response content for debugging
            except Exception as e:
                print(f"An error occurred: {str(e)}")

                return

        call_endpoint(X)

        return
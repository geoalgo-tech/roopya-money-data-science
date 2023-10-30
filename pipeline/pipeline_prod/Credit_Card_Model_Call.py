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


def Credit_Card_Model_Call(df_request_json, token):

    df = df_request_json.copy()

    # Function to convert year month (str) to total months (int)
    def convert_to_months(duration_str):
        pattern = r'(\d+)\s*yrs\s+(\d+)\s*mon'
        match = re.match(pattern, duration_str, re.IGNORECASE)
        if match:
            years = int(match.group(1))
            months = int(match.group(2))
            total_months = years * 12 + months
            return total_months
        else:
            return None

    # Apply the function to the AVERAGE_ACCOUNT_AGE column
    df['AVERAGE_ACCOUNT_AGE_IN_MONTHS'] = df['AVERAGE_ACCOUNT_AGE'].apply(lambda x: convert_to_months(x))

    # Drop the column with the uniques credit report id 
    df = df.drop(columns = ['CREDIT_REPORT_ID', 'OVERDUE_AMOUNT', 'AVERAGE_ACCOUNT_AGE'], axis = 1)

    #print(df.columns)
    #print('Subh', df['ACCOUNT_TYPE_Credit Card'])

    # Keep the rows where a customer has atleast one Credit Card

    df = df[df['ACCOUNT_TYPE_Credit Card'] != 0]

    #print("Mama:", df)

    # Resetting index after some rows are deleted
    df = df.reset_index(drop=True)

    # Replace the str value Good by 1 and Bad by 0 for final modelling
    df['ROOPYA_CUSTOMER_STATUS'] = df['ROOPYA_CUSTOMER_STATUS'].replace({'Bad': 0, 'Good': 1})


    median_value = df['AGE_COHORT'].median()

    # Replace null values with the median value
    df['AGE_COHORT'].fillna(median_value, inplace=True)

    # Columns to scalling and replacing outliers
    columns_to_deal = ['ACCOUNT_TYPE_Auto_Loan', 'ACCOUNT_TYPE_Credit Card', 'ACCOUNT_TYPE_Home_Loan', 'ACCOUNT_TYPE_Personal_Loan', 'PRIMARY_NO_OF_ACCOUNTS', 'PRIMARY_ACTIVE_ACCOUNTS', 'PRIMARY_OVERDUE_ACCOUNTS', 'PRIMARY_CURRENT_BALANCE', 'PRIMARY_SANCTIONED_AMOUNT', 'PRIMARY_DISTRIBUTED_AMOUNT', 'PRIMARY_INSTALLMENT_AMOUNT', 'NEW_ACCOUNTS_IN_LAST_SIX_MONTHS', 'NO_OF_INQUIRIES', 'Active_CURRENT_BALANCE', 'Active_OVERDUE_AMOUNT',  'INCOME', 'CONTRIBUTOR_TYPE_NBF', 'CONTRIBUTOR_TYPE_PRB', 'OWNERSHIP_IND_Guarantor', 'OWNERSHIP_IND_Individual', 'OWNERSHIP_IND_Joint', 'OWNERSHIP_IND_Supl_Card_Holder', 'ACCOUNT_STATUS_Active', 'ACCOUNT_STATUS_Closed', 'AVERAGE_ACCOUNT_AGE_IN_MONTHS']
    


    df1 = df.drop(["ROOPYA_CUSTOMER_STATUS", "STATE"], axis = 1)

    scaler = StandardScaler()

    print('Scaled data initiation')



    df_3 = df['ROOPYA_CUSTOMER_STATUS']


    # Reseting index for concatinating 
    df_1 = df1.reset_index()


    print('df1',df1.head())
    print('df1 columns:',df1.columns)
    #Concatination
    sg_df1 = pd.concat([df_1, df_3], axis = 1)
    #sg_df = pd.concat([sg_df1, df_3], axis = 1)
    sg_df = sg_df1.drop(columns = 'index', axis = 1)

    label_encoder = preprocessing.LabelEncoder()
  
    # Encode labels in column 'species'.
    sg_df['AGE_COHORT']= label_encoder.fit_transform(sg_df['AGE_COHORT'])

    X = sg_df.drop(columns = ['PRIMARY_SANCTIONED_AMOUNT', 'CURRENT_BALANCE', 'ROOPYA_CUSTOMER_STATUS', 'CCC', 'COP', 'FRB', 'HFC', 'NAB',	'Delinquent', 'Written Off',  'Delinquent_CURRENT_BALANCE','Written Off_CURRENT_BALANCE', 'Delinquent_OVERDUE_AMOUNT', 'Written Off_OVERDUE_AMOUNT'], axis = 1)

    # df12 = X

    # base_filename_X = 'CREDIT_CARD_Independent variable data'
    # object_path_X = f'Swarnavo/Pipeline/{base_filename_X}_{token}.csv'
    # csv_data_df12 = df12.to_csv(index=False)
    # bucket = storage_client.get_bucket(bucket_name)

    # blob = bucket.blob(object_path_X)
    # blob.upload_from_string(csv_data_df12, content_type='text/csv')

    # print(f'Independent variable data DataFrame saved to GCS: gs://{base_filename_X}/{object_path_X}')

    # print('Independent variable data uploaded sucessfully')

    # print("Model level Data Preprocessing successful")

    def call_endpoint(X):

        payload = X.to_json(orient='records')


        # base_filename_json = 'Payload'
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
        API_URL = 'https://asia-south1-aiplatform.googleapis.com/v1/projects/303650502197/locations/asia-south1/endpoints/384433245535600640:predict'
        headers = {'Content-Type': 'application/json', 'Authorization': 'Bearer ' + creds.token}

        try:
            response = requests.post(API_URL, data=json_request, headers=headers)
            
            # Check for a successful response (HTTP status code 200)
            if response.status_code == 200:
                print("Request was successful")
                print(response.json())  # Print the API response
                prediction_data = response.json()
                base_filename_prediction = 'CREDITCARD_Prediction'
                object_path_prediction = f'Swarnavo/Pipeline/{base_filename_prediction}_{token}.json'
                json_data = response.json() 
                bucket = storage_client.get_bucket(bucket_name)

                blob = bucket.blob(object_path_prediction)
                blob.upload_from_string(json.dumps(prediction_data), content_type='application/json')

                print('CREDITCARD_Prediction uploaded sucessfully')
                print('Endpoint hitted succesfully')

                prediction_str = ("The prediction is: {json.dumps(prediction_data)}", 200)

                return prediction_str
            else:
                print(f"Request failed with status code: {response.status_code}")
                print(response.text)  # Print the response content for debugging
        except Exception as e:
            print(f"An error occurred: {str(e)}")

            return ("error", 400)

    call_endpoint(X)

    return
import functions_framework
import json
import string
import random
import re
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
base_filename = 'PYTHON_PREPROCESS1'
base_filename_product = 'PRODUCT'

# Define the schema for the BigQuery table
#Input Schema
SCHEMA = [
            bigquery.SchemaField('CREDT_RPT_ID', 'STRING'),
            bigquery.SchemaField('STATUS', 'STRING'),
            bigquery.SchemaField('ERROR', 'STRING'),
            bigquery.SchemaField('PERFORM_CNS_SCORE', 'INT64'),
            bigquery.SchemaField('PERFORM_CNS_SCORE_DESCRIPTION', 'STRING'),
            bigquery.SchemaField('INCOME_BAND', 'STRING'),
            bigquery.SchemaField('INCOME_BAND_DESCRIPTION', 'STRING'),
            bigquery.SchemaField('PRIMARY_NO_OF_ACCOUNTS', 'FLOAT64'),
            bigquery.SchemaField('PRIMARY_ACTIVE_ACCOUNTS', 'FLOAT64'),
            bigquery.SchemaField('PRIMARY_OVERDUE_ACCOUNTS', 'FLOAT64'),
            bigquery.SchemaField('PRIMARY_CURRENT_BALANCE', 'FLOAT64'),
            bigquery.SchemaField('PRIMARY_SANCTIONED_AMOUNT', 'FLOAT64'),
            bigquery.SchemaField('PRIMARY_DISTRIBUTED_AMOUNT', 'FLOAT64'),
            bigquery.SchemaField('SECONDARY_NO_OF_ACCOUNTS', 'FLOAT64'),
            bigquery.SchemaField('SECONDARY_ACTIVE_ACCOUNTS', 'FLOAT64'),
            bigquery.SchemaField('SECONDARY_OVERDUE_ACCOUNTS', 'FLOAT64'),
            bigquery.SchemaField('SECONDARY_CURRENT_BALANCE', 'FLOAT64'),
            bigquery.SchemaField('SECONDARY_SANCTIONED_AMOUNT', 'FLOAT64'),
            bigquery.SchemaField('SECONDARY_DISBURSED_AMOUNT', 'FLOAT64'),
            bigquery.SchemaField('PRIMARY_INSTALLMENT_AMOUNT', 'FLOAT64'),
            bigquery.SchemaField('SECONDARY_INSTALLMENT_AMOUNT', 'FLOAT64'),
            bigquery.SchemaField('NEW_ACCOUNTS_IN_LAST_SIX_MONTHS', 'FLOAT64'),
            bigquery.SchemaField('DELINQUENT_ACCOUNTS_IN_LAST_SIX_MONTHS', 'FLOAT64'),
            bigquery.SchemaField('AVERAGE_ACCOUNT_AGE', 'STRING'),
            bigquery.SchemaField('CREDIT_HISTORY_LENGTH', 'STRING'),
            bigquery.SchemaField('NO_OF_INQUIRIES', 'FLOAT64'),
            bigquery.SchemaField('BRANCH', 'STRING'),
            bigquery.SchemaField('KENDRA', 'STRING'),
            bigquery.SchemaField('SELF_INDICATOR', 'BOOL'),
            bigquery.SchemaField('MATCH_TYPE', 'STRING'),
            bigquery.SchemaField('ACCOUNT_NUMBER', 'STRING'),
            bigquery.SchemaField('ACCOUNT_TYPE', 'STRING'),
            bigquery.SchemaField('CONTRIBUTOR_TYPE', 'STRING'),
            bigquery.SchemaField('DATE_REPORTED', 'DATE'),
            bigquery.SchemaField('OWNERSHIP_IND', 'STRING'),
            bigquery.SchemaField('ACCOUNT_STATUS', 'STRING'),
            bigquery.SchemaField('DISBURSED_DATE', 'DATE'),
            bigquery.SchemaField('CLOSE_DATE', 'DATE'),
            bigquery.SchemaField('LAST_PAYMENT_DATE', 'DATE'),
            bigquery.SchemaField('CREDIT_LIMIT_SANCTION_AMOUNT', 'INT64'),
            bigquery.SchemaField('DISBURSED_AMOUNT_HIGH_CREDIT', 'INT64'),
            bigquery.SchemaField('INSTALLMENT_AMOUNT', 'STRING'),
            bigquery.SchemaField('CURRENT_BALANCE', 'INT64'),
            bigquery.SchemaField('INSTALLMENT_FREQUENCY', 'STRING'),
            bigquery.SchemaField('WRITE_OFF_DATE', 'STRING'),
            bigquery.SchemaField('OVERDUE_AMOUNT', 'INT64'),
            bigquery.SchemaField('WRITE_OFF_AMOUNT', 'FLOAT64'),
            bigquery.SchemaField('ASSET_CLASS', 'STRING'),
            bigquery.SchemaField('ACCOUNT_REMARKS', 'STRING'),
            bigquery.SchemaField('LINKED_ACCOUNTS', 'BOOL'),
            bigquery.SchemaField('REPORTED_DATE_HISTORY', 'FLOAT64'),
            bigquery.SchemaField('DAYS_PAST_DUE_HISTORY', 'STRING'),
            bigquery.SchemaField('ASSET_CLASS_HISTORY', 'STRING'),
            bigquery.SchemaField('HIGH_CREDIT_HISTORY', 'STRING'),
            bigquery.SchemaField('CURRENT_BALANCE_HISTORY', 'STRING'),
            bigquery.SchemaField('DERIVED_ACCOUNT_STATUS_HISTORY', 'STRING'),
            bigquery.SchemaField('AMOUNT_OVERDUE_HISTORY', 'STRING'),
            bigquery.SchemaField('AMOUNT_PAID_HISTORY', 'STRING'),
            bigquery.SchemaField('INCOME', 'FLOAT64'),
            bigquery.SchemaField('INCOME_INDICATOR', 'STRING'),
            bigquery.SchemaField('TENURE', 'FLOAT64'),
            bigquery.SchemaField('OCCUPATION', 'STRING'),
            bigquery.SchemaField('CREDIT_GRANTOR', 'STRING'),
            bigquery.SchemaField('INQUIRY_DATE', 'DATE'),
            bigquery.SchemaField('OWNERSHIP_TYPE', 'STRING'),
            bigquery.SchemaField('PURPOSE', 'STRING'),
            bigquery.SchemaField('AMOUNT', 'INT64'),
            bigquery.SchemaField('REMARKS', 'STRING'),
            bigquery.SchemaField('BRANCH_ID', 'STRING'),
            bigquery.SchemaField('DATE_OF_BIRTH', 'STRING'),
            bigquery.SchemaField('GENDER', 'STRING'),
            bigquery.SchemaField('STATE_1', 'STRING'),
            bigquery.SchemaField('STATE_2', 'STRING'),
            bigquery.SchemaField('STATE_3', 'STRING')
]
#Schema to save after first BQ
SCHEMA1 = [
                bigquery.SchemaField('CREDIT_REPORT_ID', 'STRING'),
                bigquery.SchemaField('INCOME_BAND', 'STRING'),
                bigquery.SchemaField('PRIMARY_NO_OF_ACCOUNTS', 'FLOAT64'),
                bigquery.SchemaField('PRIMARY_ACTIVE_ACCOUNTS', 'FLOAT64'),
                bigquery.SchemaField('PRIMARY_OVERDUE_ACCOUNTS', 'FLOAT64'),
                bigquery.SchemaField('PRIMARY_CURRENT_BALANCE', 'FLOAT64'),
                bigquery.SchemaField('PRIMARY_SANCTIONED_AMOUNT', 'FLOAT64'),
                bigquery.SchemaField('PRIMARY_DISTRIBUTED_AMOUNT', 'FLOAT64'),
                bigquery.SchemaField('PRIMARY_INSTALLMENT_AMOUNT', 'FLOAT64'),
                bigquery.SchemaField('NEW_ACCOUNTS_IN_LAST_SIX_MONTHS', 'FLOAT64'),
                bigquery.SchemaField('AVERAGE_ACCOUNT_AGE', 'STRING'),
                bigquery.SchemaField('NO_OF_INQUIRIES', 'FLOAT64'),
                bigquery.SchemaField('SELF_INDICATOR', 'BOOL'),
                bigquery.SchemaField('MATCH_TYPE', 'STRING'),
                bigquery.SchemaField('ACCOUNT_TYPE', 'STRING'),
                bigquery.SchemaField('CONTRIBUTOR_TYPE', 'STRING'),
                bigquery.SchemaField('OWNERSHIP_IND', 'STRING'),
                bigquery.SchemaField('DATE_REPORTED', 'DATE'),
                bigquery.SchemaField('ACCOUNT_STATUS', 'STRING'),
                bigquery.SchemaField('DISBURSED_DATE', 'DATE'),
                bigquery.SchemaField('CURRENT_BALANCE', 'INT64'),
                bigquery.SchemaField('OVERDUE_AMOUNT', 'INT64'),
                bigquery.SchemaField('DAYS_PAST_DUE_HISTORY_MONTH_1', 'STRING'),
                bigquery.SchemaField('DAYS_PAST_DUE_HISTORY_MONTH_2', 'STRING'),
                bigquery.SchemaField('DAYS_PAST_DUE_HISTORY_MONTH_3', 'STRING'),
                bigquery.SchemaField('DAYS_PAST_DUE_HISTORY_MONTH_4', 'STRING'),
                bigquery.SchemaField('DAYS_PAST_DUE_HISTORY_MONTH_5', 'STRING'),
                bigquery.SchemaField('DAYS_PAST_DUE_HISTORY_MONTH_6', 'STRING'),
                bigquery.SchemaField('DAYS_PAST_DUE_HISTORY_MONTH_7', 'STRING'),
                bigquery.SchemaField('DAYS_PAST_DUE_HISTORY_MONTH_8', 'STRING'),
                bigquery.SchemaField('DAYS_PAST_DUE_HISTORY_MONTH_9', 'STRING'),
                bigquery.SchemaField('DAYS_PAST_DUE_HISTORY_MONTH_10', 'STRING'),
                bigquery.SchemaField('DAYS_PAST_DUE_HISTORY_MONTH_11', 'STRING'),
                bigquery.SchemaField('DAYS_PAST_DUE_HISTORY_MONTH_12', 'STRING'),
                bigquery.SchemaField('DAYS_PAST_DUE_HISTORY_MONTH_13', 'STRING'),
                bigquery.SchemaField('DAYS_PAST_DUE_HISTORY_MONTH_14', 'STRING'),
                bigquery.SchemaField('DAYS_PAST_DUE_HISTORY_MONTH_15', 'STRING'),
                bigquery.SchemaField('DAYS_PAST_DUE_HISTORY_MONTH_16', 'STRING'),
                bigquery.SchemaField('DAYS_PAST_DUE_HISTORY_MONTH_17', 'STRING'),
                bigquery.SchemaField('DAYS_PAST_DUE_HISTORY_MONTH_18', 'STRING'),
                bigquery.SchemaField('DAYS_PAST_DUE_HISTORY_MONTH_19', 'STRING'),
                bigquery.SchemaField('DAYS_PAST_DUE_HISTORY_MONTH_20', 'STRING'),
                bigquery.SchemaField('DAYS_PAST_DUE_HISTORY_MONTH_21', 'STRING'),
                bigquery.SchemaField('DAYS_PAST_DUE_HISTORY_MONTH_22', 'STRING'),
                bigquery.SchemaField('DAYS_PAST_DUE_HISTORY_MONTH_23', 'STRING'),
                bigquery.SchemaField('DAYS_PAST_DUE_HISTORY_MONTH_24', 'STRING'),
                bigquery.SchemaField('DAYS_PAST_DUE_HISTORY_MONTH_25', 'STRING'),
                bigquery.SchemaField('DAYS_PAST_DUE_HISTORY_MONTH_26', 'STRING'),
                bigquery.SchemaField('DAYS_PAST_DUE_HISTORY_MONTH_27', 'STRING'),
                bigquery.SchemaField('DAYS_PAST_DUE_HISTORY_MONTH_28', 'STRING'),
                bigquery.SchemaField('DAYS_PAST_DUE_HISTORY_MONTH_29', 'STRING'),
                bigquery.SchemaField('DAYS_PAST_DUE_HISTORY_MONTH_30', 'STRING'),
                bigquery.SchemaField('DAYS_PAST_DUE_HISTORY_MONTH_31', 'STRING'),
                bigquery.SchemaField('DAYS_PAST_DUE_HISTORY_MONTH_32', 'STRING'),
                bigquery.SchemaField('DAYS_PAST_DUE_HISTORY_MONTH_33', 'STRING'),
                bigquery.SchemaField('DAYS_PAST_DUE_HISTORY_MONTH_34', 'STRING'),
                bigquery.SchemaField('DAYS_PAST_DUE_HISTORY_MONTH_35', 'STRING'),
                bigquery.SchemaField('DAYS_PAST_DUE_HISTORY_MONTH_36', 'STRING'),
                bigquery.SchemaField('AMOUNT_OVERDUE_HISTORY_MONTH_1', 'FLOAT64'),
                bigquery.SchemaField('AMOUNT_OVERDUE_HISTORY_MONTH_2', 'FLOAT64'),
                bigquery.SchemaField('AMOUNT_OVERDUE_HISTORY_MONTH_3', 'FLOAT64'),
                bigquery.SchemaField('AMOUNT_OVERDUE_HISTORY_MONTH_4', 'FLOAT64'),
                bigquery.SchemaField('AMOUNT_OVERDUE_HISTORY_MONTH_5', 'FLOAT64'),
                bigquery.SchemaField('AMOUNT_OVERDUE_HISTORY_MONTH_6', 'FLOAT64'),
                bigquery.SchemaField('AMOUNT_OVERDUE_HISTORY_MONTH_7', 'FLOAT64'),
                bigquery.SchemaField('AMOUNT_OVERDUE_HISTORY_MONTH_8', 'FLOAT64'),
                bigquery.SchemaField('AMOUNT_OVERDUE_HISTORY_MONTH_9', 'FLOAT64'),
                bigquery.SchemaField('AMOUNT_OVERDUE_HISTORY_MONTH_10', 'FLOAT64'),
                bigquery.SchemaField('AMOUNT_OVERDUE_HISTORY_MONTH_11', 'FLOAT64'),
                bigquery.SchemaField('AMOUNT_OVERDUE_HISTORY_MONTH_12', 'FLOAT64'),
                bigquery.SchemaField('AMOUNT_OVERDUE_HISTORY_MONTH_13', 'FLOAT64'),
                bigquery.SchemaField('AMOUNT_OVERDUE_HISTORY_MONTH_14', 'FLOAT64'),
                bigquery.SchemaField('AMOUNT_OVERDUE_HISTORY_MONTH_15', 'FLOAT64'),
                bigquery.SchemaField('AMOUNT_OVERDUE_HISTORY_MONTH_16', 'FLOAT64'),
                bigquery.SchemaField('AMOUNT_OVERDUE_HISTORY_MONTH_17', 'FLOAT64'),
                bigquery.SchemaField('AMOUNT_OVERDUE_HISTORY_MONTH_18', 'FLOAT64'),
                bigquery.SchemaField('AMOUNT_OVERDUE_HISTORY_MONTH_19', 'FLOAT64'),
                bigquery.SchemaField('AMOUNT_OVERDUE_HISTORY_MONTH_20', 'FLOAT64'),
                bigquery.SchemaField('AMOUNT_OVERDUE_HISTORY_MONTH_21', 'FLOAT64'),
                bigquery.SchemaField('AMOUNT_OVERDUE_HISTORY_MONTH_22', 'FLOAT64'),
                bigquery.SchemaField('AMOUNT_OVERDUE_HISTORY_MONTH_23', 'FLOAT64'),
                bigquery.SchemaField('AMOUNT_OVERDUE_HISTORY_MONTH_24', 'FLOAT64'),
                bigquery.SchemaField('AMOUNT_OVERDUE_HISTORY_MONTH_25', 'FLOAT64'),
                bigquery.SchemaField('AMOUNT_OVERDUE_HISTORY_MONTH_26', 'FLOAT64'),
                bigquery.SchemaField('AMOUNT_OVERDUE_HISTORY_MONTH_27', 'FLOAT64'),
                bigquery.SchemaField('AMOUNT_OVERDUE_HISTORY_MONTH_28', 'FLOAT64'),
                bigquery.SchemaField('AMOUNT_OVERDUE_HISTORY_MONTH_29', 'FLOAT64'),
                bigquery.SchemaField('AMOUNT_OVERDUE_HISTORY_MONTH_30', 'FLOAT64'),
                bigquery.SchemaField('AMOUNT_OVERDUE_HISTORY_MONTH_31', 'FLOAT64'),
                bigquery.SchemaField('AMOUNT_OVERDUE_HISTORY_MONTH_32', 'FLOAT64'),
                bigquery.SchemaField('AMOUNT_OVERDUE_HISTORY_MONTH_33', 'FLOAT64'),
                bigquery.SchemaField('AMOUNT_OVERDUE_HISTORY_MONTH_34', 'FLOAT64'),
                bigquery.SchemaField('AMOUNT_OVERDUE_HISTORY_MONTH_35', 'FLOAT64'),
                bigquery.SchemaField('AMOUNT_OVERDUE_HISTORY_MONTH_36', 'FLOAT64'),
                bigquery.SchemaField('INCOME', 'FLOAT64'),
                bigquery.SchemaField('AMOUNT', 'INT64'),
                bigquery.SchemaField('AGE_COHORT', 'INT64'),
                bigquery.SchemaField('GENDER', 'STRING'),
                bigquery.SchemaField('STATE', 'STRING')
]

SCHEMA2 = [
                bigquery.SchemaField('CREDIT_REPORT_ID', 'STRING'),
                bigquery.SchemaField('PRIMARY_NO_OF_ACCOUNTS', 'FLOAT64'),
                bigquery.SchemaField('PRIMARY_ACTIVE_ACCOUNTS', 'FLOAT64'),
                bigquery.SchemaField('PRIMARY_OVERDUE_ACCOUNTS', 'FLOAT64'),
                bigquery.SchemaField('PRIMARY_CURRENT_BALANCE', 'FLOAT64'),
                bigquery.SchemaField('PRIMARY_SANCTIONED_AMOUNT', 'FLOAT64'),
                bigquery.SchemaField('PRIMARY_DISTRIBUTED_AMOUNT', 'FLOAT64'),
                bigquery.SchemaField('PRIMARY_INSTALLMENT_AMOUNT', 'FLOAT64'),
                bigquery.SchemaField('NEW_ACCOUNTS_IN_LAST_SIX_MONTHS', 'FLOAT64'),
                bigquery.SchemaField('AVERAGE_ACCOUNT_AGE', 'STRING'),
                bigquery.SchemaField('NO_OF_INQUIRIES', 'FLOAT64'),
                bigquery.SchemaField('SELF_INDICATOR', 'BOOL'),
                bigquery.SchemaField('MATCH_TYPE', 'STRING'),
                bigquery.SchemaField('ACCOUNT_TYPE', 'STRING'),
                bigquery.SchemaField('CONTRIBUTOR_TYPE', 'STRING'),
                bigquery.SchemaField('OWNERSHIP_IND', 'STRING'),
                bigquery.SchemaField('DATE_REPORTED', 'DATE'),
                bigquery.SchemaField('ACCOUNT_STATUS', 'STRING'),
                bigquery.SchemaField('DISBURSED_DATE', 'DATE'),
                bigquery.SchemaField('CURRENT_BALANCE', 'INT64'),
                bigquery.SchemaField('OVERDUE_AMOUNT', 'INT64'),
                bigquery.SchemaField('DAYS_PAST_DUE_HISTORY_MONTH_1', 'STRING'),
                bigquery.SchemaField('DAYS_PAST_DUE_HISTORY_MONTH_2', 'STRING'),
                bigquery.SchemaField('DAYS_PAST_DUE_HISTORY_MONTH_3', 'STRING'),
                bigquery.SchemaField('DAYS_PAST_DUE_HISTORY_MONTH_4', 'STRING'),
                bigquery.SchemaField('DAYS_PAST_DUE_HISTORY_MONTH_5', 'STRING'),
                bigquery.SchemaField('DAYS_PAST_DUE_HISTORY_MONTH_6', 'STRING'),
                bigquery.SchemaField('DAYS_PAST_DUE_HISTORY_MONTH_7', 'STRING'),
                bigquery.SchemaField('DAYS_PAST_DUE_HISTORY_MONTH_8', 'STRING'),
                bigquery.SchemaField('DAYS_PAST_DUE_HISTORY_MONTH_9', 'STRING'),
                bigquery.SchemaField('DAYS_PAST_DUE_HISTORY_MONTH_10', 'STRING'),
                bigquery.SchemaField('DAYS_PAST_DUE_HISTORY_MONTH_11', 'STRING'),
                bigquery.SchemaField('DAYS_PAST_DUE_HISTORY_MONTH_12', 'STRING'),
                bigquery.SchemaField('DAYS_PAST_DUE_HISTORY_MONTH_13', 'STRING'),
                bigquery.SchemaField('DAYS_PAST_DUE_HISTORY_MONTH_14', 'STRING'),
                bigquery.SchemaField('DAYS_PAST_DUE_HISTORY_MONTH_15', 'STRING'),
                bigquery.SchemaField('DAYS_PAST_DUE_HISTORY_MONTH_16', 'STRING'),
                bigquery.SchemaField('DAYS_PAST_DUE_HISTORY_MONTH_17', 'STRING'),
                bigquery.SchemaField('DAYS_PAST_DUE_HISTORY_MONTH_18', 'STRING'),
                bigquery.SchemaField('DAYS_PAST_DUE_HISTORY_MONTH_19', 'STRING'),
                bigquery.SchemaField('DAYS_PAST_DUE_HISTORY_MONTH_20', 'STRING'),
                bigquery.SchemaField('DAYS_PAST_DUE_HISTORY_MONTH_21', 'STRING'),
                bigquery.SchemaField('DAYS_PAST_DUE_HISTORY_MONTH_22', 'STRING'),
                bigquery.SchemaField('DAYS_PAST_DUE_HISTORY_MONTH_23', 'STRING'),
                bigquery.SchemaField('DAYS_PAST_DUE_HISTORY_MONTH_24', 'STRING'),
                bigquery.SchemaField('DAYS_PAST_DUE_HISTORY_MONTH_25', 'STRING'),
                bigquery.SchemaField('DAYS_PAST_DUE_HISTORY_MONTH_26', 'STRING'),
                bigquery.SchemaField('DAYS_PAST_DUE_HISTORY_MONTH_27', 'STRING'),
                bigquery.SchemaField('DAYS_PAST_DUE_HISTORY_MONTH_28', 'STRING'),
                bigquery.SchemaField('DAYS_PAST_DUE_HISTORY_MONTH_29', 'STRING'),
                bigquery.SchemaField('DAYS_PAST_DUE_HISTORY_MONTH_30', 'STRING'),
                bigquery.SchemaField('DAYS_PAST_DUE_HISTORY_MONTH_31', 'STRING'),
                bigquery.SchemaField('DAYS_PAST_DUE_HISTORY_MONTH_32', 'STRING'),
                bigquery.SchemaField('DAYS_PAST_DUE_HISTORY_MONTH_33', 'STRING'),
                bigquery.SchemaField('DAYS_PAST_DUE_HISTORY_MONTH_34', 'STRING'),
                bigquery.SchemaField('DAYS_PAST_DUE_HISTORY_MONTH_35', 'STRING'),
                bigquery.SchemaField('DAYS_PAST_DUE_HISTORY_MONTH_36', 'STRING'),
                bigquery.SchemaField('AMOUNT_OVERDUE_HISTORY_MONTH_1', 'FLOAT64'),
                bigquery.SchemaField('AMOUNT_OVERDUE_HISTORY_MONTH_2', 'FLOAT64'),
                bigquery.SchemaField('AMOUNT_OVERDUE_HISTORY_MONTH_3', 'FLOAT64'),
                bigquery.SchemaField('AMOUNT_OVERDUE_HISTORY_MONTH_4', 'FLOAT64'),
                bigquery.SchemaField('AMOUNT_OVERDUE_HISTORY_MONTH_5', 'FLOAT64'),
                bigquery.SchemaField('AMOUNT_OVERDUE_HISTORY_MONTH_6', 'FLOAT64'),
                bigquery.SchemaField('AMOUNT_OVERDUE_HISTORY_MONTH_7', 'FLOAT64'),
                bigquery.SchemaField('AMOUNT_OVERDUE_HISTORY_MONTH_8', 'FLOAT64'),
                bigquery.SchemaField('AMOUNT_OVERDUE_HISTORY_MONTH_9', 'FLOAT64'),
                bigquery.SchemaField('AMOUNT_OVERDUE_HISTORY_MONTH_10', 'FLOAT64'),
                bigquery.SchemaField('AMOUNT_OVERDUE_HISTORY_MONTH_11', 'FLOAT64'),
                bigquery.SchemaField('AMOUNT_OVERDUE_HISTORY_MONTH_12', 'FLOAT64'),
                bigquery.SchemaField('AMOUNT_OVERDUE_HISTORY_MONTH_13', 'FLOAT64'),
                bigquery.SchemaField('AMOUNT_OVERDUE_HISTORY_MONTH_14', 'FLOAT64'),
                bigquery.SchemaField('AMOUNT_OVERDUE_HISTORY_MONTH_15', 'FLOAT64'),
                bigquery.SchemaField('AMOUNT_OVERDUE_HISTORY_MONTH_16', 'FLOAT64'),
                bigquery.SchemaField('AMOUNT_OVERDUE_HISTORY_MONTH_17', 'FLOAT64'),
                bigquery.SchemaField('AMOUNT_OVERDUE_HISTORY_MONTH_18', 'FLOAT64'),
                bigquery.SchemaField('AMOUNT_OVERDUE_HISTORY_MONTH_19', 'FLOAT64'),
                bigquery.SchemaField('AMOUNT_OVERDUE_HISTORY_MONTH_20', 'FLOAT64'),
                bigquery.SchemaField('AMOUNT_OVERDUE_HISTORY_MONTH_21', 'FLOAT64'),
                bigquery.SchemaField('AMOUNT_OVERDUE_HISTORY_MONTH_22', 'FLOAT64'),
                bigquery.SchemaField('AMOUNT_OVERDUE_HISTORY_MONTH_23', 'FLOAT64'),
                bigquery.SchemaField('AMOUNT_OVERDUE_HISTORY_MONTH_24', 'FLOAT64'),
                bigquery.SchemaField('AMOUNT_OVERDUE_HISTORY_MONTH_25', 'FLOAT64'),
                bigquery.SchemaField('AMOUNT_OVERDUE_HISTORY_MONTH_26', 'FLOAT64'),
                bigquery.SchemaField('AMOUNT_OVERDUE_HISTORY_MONTH_27', 'FLOAT64'),
                bigquery.SchemaField('AMOUNT_OVERDUE_HISTORY_MONTH_28', 'FLOAT64'),
                bigquery.SchemaField('AMOUNT_OVERDUE_HISTORY_MONTH_29', 'FLOAT64'),
                bigquery.SchemaField('AMOUNT_OVERDUE_HISTORY_MONTH_30', 'FLOAT64'),
                bigquery.SchemaField('AMOUNT_OVERDUE_HISTORY_MONTH_31', 'FLOAT64'),
                bigquery.SchemaField('AMOUNT_OVERDUE_HISTORY_MONTH_32', 'FLOAT64'),
                bigquery.SchemaField('AMOUNT_OVERDUE_HISTORY_MONTH_33', 'FLOAT64'),
                bigquery.SchemaField('AMOUNT_OVERDUE_HISTORY_MONTH_34', 'FLOAT64'),
                bigquery.SchemaField('AMOUNT_OVERDUE_HISTORY_MONTH_35', 'FLOAT64'),
                bigquery.SchemaField('AMOUNT_OVERDUE_HISTORY_MONTH_36', 'FLOAT64'),
                bigquery.SchemaField('INCOME', 'FLOAT64'),
                bigquery.SchemaField('AMOUNT', 'INT64'),
                bigquery.SchemaField('AGE_COHORT', 'INT64'),
                bigquery.SchemaField('STATE', 'STRING')
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
        print(table_id)
        query = f'''SELECT CREDT_RPT_ID AS CREDIT_REPORT_ID,INCOME_BAND,PRIMARY_NO_OF_ACCOUNTS,PRIMARY_ACTIVE_ACCOUNTS,PRIMARY_OVERDUE_ACCOUNTS,PRIMARY_CURRENT_BALANCE,PRIMARY_SANCTIONED_AMOUNT,PRIMARY_DISTRIBUTED_AMOUNT,PRIMARY_INSTALLMENT_AMOUNT,NEW_ACCOUNTS_IN_LAST_SIX_MONTHS,AVERAGE_ACCOUNT_AGE,NO_OF_INQUIRIES,SELF_INDICATOR,MATCH_TYPE,ACCOUNT_TYPE,CONTRIBUTOR_TYPE,OWNERSHIP_IND,DATE_REPORTED,ACCOUNT_STATUS,DISBURSED_DATE,CURRENT_BALANCE,OVERDUE_AMOUNT,
                    SUBSTRING(DAYS_PAST_DUE_HISTORY, 1, 3) AS DAYS_PAST_DUE_HISTORY_MONTH_1,
                    SUBSTRING(DAYS_PAST_DUE_HISTORY, 4, 3) AS DAYS_PAST_DUE_HISTORY_MONTH_2,
                    SUBSTRING(DAYS_PAST_DUE_HISTORY,7, 3) AS DAYS_PAST_DUE_HISTORY_MONTH_3,
                    SUBSTRING(DAYS_PAST_DUE_HISTORY, 10, 3) AS DAYS_PAST_DUE_HISTORY_MONTH_4,
                    SUBSTRING(DAYS_PAST_DUE_HISTORY, 13, 3) AS DAYS_PAST_DUE_HISTORY_MONTH_5,
                    SUBSTRING(DAYS_PAST_DUE_HISTORY, 16, 3) AS DAYS_PAST_DUE_HISTORY_MONTH_6,
                    SUBSTRING(DAYS_PAST_DUE_HISTORY, 19, 3) AS DAYS_PAST_DUE_HISTORY_MONTH_7,
                    SUBSTRING(DAYS_PAST_DUE_HISTORY, 22, 3) AS DAYS_PAST_DUE_HISTORY_MONTH_8,
                    SUBSTRING(DAYS_PAST_DUE_HISTORY, 25, 3) AS DAYS_PAST_DUE_HISTORY_MONTH_9,
                    SUBSTRING(DAYS_PAST_DUE_HISTORY, 28, 3) AS DAYS_PAST_DUE_HISTORY_MONTH_10,
                    SUBSTRING(DAYS_PAST_DUE_HISTORY, 31, 3) AS DAYS_PAST_DUE_HISTORY_MONTH_11,
                    SUBSTRING(DAYS_PAST_DUE_HISTORY, 34, 3) AS DAYS_PAST_DUE_HISTORY_MONTH_12,
                    SUBSTRING(DAYS_PAST_DUE_HISTORY, 37, 3) AS DAYS_PAST_DUE_HISTORY_MONTH_13,
                    SUBSTRING(DAYS_PAST_DUE_HISTORY, 40, 3) AS DAYS_PAST_DUE_HISTORY_MONTH_14,
                    SUBSTRING(DAYS_PAST_DUE_HISTORY, 43, 3) AS DAYS_PAST_DUE_HISTORY_MONTH_15,
                    SUBSTRING(DAYS_PAST_DUE_HISTORY, 46, 3) AS DAYS_PAST_DUE_HISTORY_MONTH_16,
                    SUBSTRING(DAYS_PAST_DUE_HISTORY, 49, 3) AS DAYS_PAST_DUE_HISTORY_MONTH_17,
                    SUBSTRING(DAYS_PAST_DUE_HISTORY, 52, 3) AS DAYS_PAST_DUE_HISTORY_MONTH_18,
                    SUBSTRING(DAYS_PAST_DUE_HISTORY, 55, 3) AS DAYS_PAST_DUE_HISTORY_MONTH_19,
                    SUBSTRING(DAYS_PAST_DUE_HISTORY, 58, 3) AS DAYS_PAST_DUE_HISTORY_MONTH_20,
                    SUBSTRING(DAYS_PAST_DUE_HISTORY, 61, 3) AS DAYS_PAST_DUE_HISTORY_MONTH_21,
                    SUBSTRING(DAYS_PAST_DUE_HISTORY, 64, 3) AS DAYS_PAST_DUE_HISTORY_MONTH_22,
                    SUBSTRING(DAYS_PAST_DUE_HISTORY, 67, 3) AS DAYS_PAST_DUE_HISTORY_MONTH_23,
                    SUBSTRING(DAYS_PAST_DUE_HISTORY, 70, 3) AS DAYS_PAST_DUE_HISTORY_MONTH_24,
                    SUBSTRING(DAYS_PAST_DUE_HISTORY, 73, 3) AS DAYS_PAST_DUE_HISTORY_MONTH_25,
                    SUBSTRING(DAYS_PAST_DUE_HISTORY, 76, 3) AS DAYS_PAST_DUE_HISTORY_MONTH_26,
                    SUBSTRING(DAYS_PAST_DUE_HISTORY, 79, 3) AS DAYS_PAST_DUE_HISTORY_MONTH_27,
                    SUBSTRING(DAYS_PAST_DUE_HISTORY, 82, 3) AS DAYS_PAST_DUE_HISTORY_MONTH_28,
                    SUBSTRING(DAYS_PAST_DUE_HISTORY, 85, 3) AS DAYS_PAST_DUE_HISTORY_MONTH_29,
                    SUBSTRING(DAYS_PAST_DUE_HISTORY, 88, 3) AS DAYS_PAST_DUE_HISTORY_MONTH_30,
                    SUBSTRING(DAYS_PAST_DUE_HISTORY, 91, 3) AS DAYS_PAST_DUE_HISTORY_MONTH_31,
                    SUBSTRING(DAYS_PAST_DUE_HISTORY, 94, 3) AS DAYS_PAST_DUE_HISTORY_MONTH_32,
                    SUBSTRING(DAYS_PAST_DUE_HISTORY, 97, 3) AS DAYS_PAST_DUE_HISTORY_MONTH_33,
                    SUBSTRING(DAYS_PAST_DUE_HISTORY, 100, 3) AS DAYS_PAST_DUE_HISTORY_MONTH_34,
                    SUBSTRING(DAYS_PAST_DUE_HISTORY, 103, 3) AS DAYS_PAST_DUE_HISTORY_MONTH_35,
                    SUBSTRING(DAYS_PAST_DUE_HISTORY, 106, 3) AS DAYS_PAST_DUE_HISTORY_MONTH_36,
                    CAST(REGEXP_EXTRACT(AMOUNT_OVERDUE_HISTORY, r'([^,]+)') AS FLOAT64) AS AMOUNT_OVERDUE_HISTORY_MONTH_1,
                    CAST(REGEXP_EXTRACT(AMOUNT_OVERDUE_HISTORY, r'[^,]+,([^,]+)') AS FLOAT64) AS AMOUNT_OVERDUE_HISTORY_MONTH_2,
                    CAST(REGEXP_EXTRACT(AMOUNT_OVERDUE_HISTORY, r'[^,]+,[^,]+,([^,]+)') AS FLOAT64) AS AMOUNT_OVERDUE_HISTORY_MONTH_3,
                    CAST(REGEXP_EXTRACT(AMOUNT_OVERDUE_HISTORY, r'[^,]+,[^,]+,[^,]+,([^,]+)') AS FLOAT64) AS AMOUNT_OVERDUE_HISTORY_MONTH_4,
                    CAST(REGEXP_EXTRACT(AMOUNT_OVERDUE_HISTORY, r'[^,]+,[^,]+,[^,]+,[^,]+,([^,]+)') AS FLOAT64) AS AMOUNT_OVERDUE_HISTORY_MONTH_5,
                    CAST(REGEXP_EXTRACT(AMOUNT_OVERDUE_HISTORY, r'[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,([^,]+)') AS FLOAT64) AS AMOUNT_OVERDUE_HISTORY_MONTH_6,
                    CAST(REGEXP_EXTRACT(AMOUNT_OVERDUE_HISTORY, r'[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,([^,]+)') AS FLOAT64) AS AMOUNT_OVERDUE_HISTORY_MONTH_7,
                    CAST(REGEXP_EXTRACT(AMOUNT_OVERDUE_HISTORY, r'[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,([^,]+)') AS FLOAT64) AS AMOUNT_OVERDUE_HISTORY_MONTH_8,
                    CAST(REGEXP_EXTRACT(AMOUNT_OVERDUE_HISTORY, r'[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,([^,]+)') AS FLOAT64) AS AMOUNT_OVERDUE_HISTORY_MONTH_9,
                    CAST(REGEXP_EXTRACT(AMOUNT_OVERDUE_HISTORY, r'[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,([^,]+)') AS FLOAT64) AS AMOUNT_OVERDUE_HISTORY_MONTH_10,
                    CAST(REGEXP_EXTRACT(AMOUNT_OVERDUE_HISTORY, r'[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,([^,]+)') AS FLOAT64) AS AMOUNT_OVERDUE_HISTORY_MONTH_11,
                    CAST(REGEXP_EXTRACT(AMOUNT_OVERDUE_HISTORY, r'[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,([^,]+)') AS FLOAT64) AS AMOUNT_OVERDUE_HISTORY_MONTH_12,
                    CAST(REGEXP_EXTRACT(AMOUNT_OVERDUE_HISTORY, r'[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,([^,]+)') AS FLOAT64) AS AMOUNT_OVERDUE_HISTORY_MONTH_13,
                    CAST(REGEXP_EXTRACT(AMOUNT_OVERDUE_HISTORY, r'[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,([^,]+)') AS FLOAT64) AS AMOUNT_OVERDUE_HISTORY_MONTH_14,
                    CAST(REGEXP_EXTRACT(AMOUNT_OVERDUE_HISTORY, r'[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,([^,]+)') AS FLOAT64) AS AMOUNT_OVERDUE_HISTORY_MONTH_15,
                    CAST(REGEXP_EXTRACT(AMOUNT_OVERDUE_HISTORY, r'[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,([^,]+)') AS FLOAT64) AS AMOUNT_OVERDUE_HISTORY_MONTH_16,
                    CAST(REGEXP_EXTRACT(AMOUNT_OVERDUE_HISTORY, r'[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,([^,]+)') AS FLOAT64) AS AMOUNT_OVERDUE_HISTORY_MONTH_17,
                    CAST(REGEXP_EXTRACT(AMOUNT_OVERDUE_HISTORY, r'[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,([^,]+)') AS FLOAT64) AS AMOUNT_OVERDUE_HISTORY_MONTH_18,
                    CAST(REGEXP_EXTRACT(AMOUNT_OVERDUE_HISTORY, r'[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,([^,]+)') AS FLOAT64) AS AMOUNT_OVERDUE_HISTORY_MONTH_19,
                    CAST(REGEXP_EXTRACT(AMOUNT_OVERDUE_HISTORY, r'[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,([^,]+)') AS FLOAT64) AS AMOUNT_OVERDUE_HISTORY_MONTH_20,
                    CAST(REGEXP_EXTRACT(AMOUNT_OVERDUE_HISTORY, r'[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,([^,]+)') AS FLOAT64) AS AMOUNT_OVERDUE_HISTORY_MONTH_21,
                    CAST(REGEXP_EXTRACT(AMOUNT_OVERDUE_HISTORY, r'[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,([^,]+)') AS FLOAT64) AS AMOUNT_OVERDUE_HISTORY_MONTH_22,
                    CAST(REGEXP_EXTRACT(AMOUNT_OVERDUE_HISTORY, r'[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,([^,]+)') AS FLOAT64) AS AMOUNT_OVERDUE_HISTORY_MONTH_23,
                    CAST(REGEXP_EXTRACT(AMOUNT_OVERDUE_HISTORY, r'[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,([^,]+)') AS FLOAT64) AS AMOUNT_OVERDUE_HISTORY_MONTH_24,
                    CAST(REGEXP_EXTRACT(AMOUNT_OVERDUE_HISTORY, r'[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,([^,]+)') AS FLOAT64) AS AMOUNT_OVERDUE_HISTORY_MONTH_25,
                    CAST(REGEXP_EXTRACT(AMOUNT_OVERDUE_HISTORY, r'[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,([^,]+)') AS FLOAT64) AS AMOUNT_OVERDUE_HISTORY_MONTH_26,
                    CAST(REGEXP_EXTRACT(AMOUNT_OVERDUE_HISTORY, r'[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,([^,]+)') AS FLOAT64) AS AMOUNT_OVERDUE_HISTORY_MONTH_27,
                    CAST(REGEXP_EXTRACT(AMOUNT_OVERDUE_HISTORY, r'[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,([^,]+)') AS FLOAT64) AS AMOUNT_OVERDUE_HISTORY_MONTH_28,
                    CAST(REGEXP_EXTRACT(AMOUNT_OVERDUE_HISTORY, r'[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,([^,]+)') AS FLOAT64) AS AMOUNT_OVERDUE_HISTORY_MONTH_29,
                    CAST(REGEXP_EXTRACT(AMOUNT_OVERDUE_HISTORY, r'[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,([^,]+)') AS FLOAT64) AS AMOUNT_OVERDUE_HISTORY_MONTH_30,
                    CAST(REGEXP_EXTRACT(AMOUNT_OVERDUE_HISTORY, r'[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,([^,]+)') AS FLOAT64) AS AMOUNT_OVERDUE_HISTORY_MONTH_31,
                    CAST(REGEXP_EXTRACT(AMOUNT_OVERDUE_HISTORY, r'[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,([^,]+)') AS FLOAT64) AS AMOUNT_OVERDUE_HISTORY_MONTH_32,
                    CAST(REGEXP_EXTRACT(AMOUNT_OVERDUE_HISTORY, r'[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,([^,]+)') AS FLOAT64) AS AMOUNT_OVERDUE_HISTORY_MONTH_33,
                    CAST(REGEXP_EXTRACT(AMOUNT_OVERDUE_HISTORY, r'[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,([^,]+)') AS FLOAT64) AS AMOUNT_OVERDUE_HISTORY_MONTH_34,
                    CAST(REGEXP_EXTRACT(AMOUNT_OVERDUE_HISTORY, r'[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,([^,]+)') AS FLOAT64) AS AMOUNT_OVERDUE_HISTORY_MONTH_35,
                    CAST(REGEXP_EXTRACT(AMOUNT_OVERDUE_HISTORY, r'[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,([^,]+)') AS FLOAT64) AS AMOUNT_OVERDUE_HISTORY_MONTH_36,
                    INCOME,AMOUNT,
                    CASE
                     WHEN PARSE_DATE('%d-%m-%Y',DATE_OF_BIRTH) BETWEEN DATE('1936-12-10') AND DATE('1967-12-31') THEN 25
                     WHEN PARSE_DATE('%d-%m-%Y',DATE_OF_BIRTH) BETWEEN DATE('1968-01-01') AND DATE('1972-12-31') THEN 55
                     WHEN PARSE_DATE('%d-%m-%Y',DATE_OF_BIRTH) BETWEEN DATE('1973-01-01') AND DATE('1977-12-31') THEN 50
                     WHEN PARSE_DATE('%d-%m-%Y',DATE_OF_BIRTH) BETWEEN DATE('1978-01-01') AND DATE('1982-12-31') THEN 45
                     WHEN PARSE_DATE('%d-%m-%Y',DATE_OF_BIRTH) BETWEEN DATE('1983-01-01') AND DATE('1987-12-31') THEN 40
                     WHEN PARSE_DATE('%d-%m-%Y',DATE_OF_BIRTH) BETWEEN DATE('1988-01-01') AND DATE('1992-12-31') THEN 35
                     WHEN PARSE_DATE('%d-%m-%Y',DATE_OF_BIRTH) BETWEEN DATE('1993-01-01') AND DATE('1997-12-31') THEN 30
                     WHEN PARSE_DATE('%d-%m-%Y',DATE_OF_BIRTH) BETWEEN DATE('1998-01-01') AND DATE('2002-12-31') THEN 25
                     WHEN PARSE_DATE('%d-%m-%Y',DATE_OF_BIRTH) BETWEEN DATE('2003-01-01') AND DATE('2005-12-31') THEN 25
                     ELSE NULL
                    END AS AGE_COHORT,
                    GENDER,STATE_1 AS STATE
                    FROM `{PROJECT_ID}.{DATASET_ID}.{table_id}`
                    WHERE DATE_DIFF(CURRENT_DATE(), DATE(DATE_REPORTED), MONTH) <= 36 AND SUBSTRING(DAYS_PAST_DUE_HISTORY, 1, 3) IS NOT NULL AND CAST(REGEXP_EXTRACT(AMOUNT_OVERDUE_HISTORY, r'([^,]+)') AS FLOAT64) IS NOT NULL AND ACCOUNT_STATUS IN ('Active', 'Closed') 
                    AND STATE_1 is not Null 
                    AND CONTRIBUTOR_TYPE iN ('NBF', 'PRB');'''
        
        preprocess_table_id = 'PREPROCESS1_' + ''.join(token)
        #Create a reference to the preprocess1 table
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
    query = f'''WITH RankedCreditReports AS (
                SELECT CREDIT_REPORT_ID,PRIMARY_NO_OF_ACCOUNTS,PRIMARY_ACTIVE_ACCOUNTS,PRIMARY_OVERDUE_ACCOUNTS,PRIMARY_CURRENT_BALANCE,PRIMARY_SANCTIONED_AMOUNT,PRIMARY_DISTRIBUTED_AMOUNT,PRIMARY_INSTALLMENT_AMOUNT,NEW_ACCOUNTS_IN_LAST_SIX_MONTHS,AVERAGE_ACCOUNT_AGE,NO_OF_INQUIRIES,SELF_INDICATOR,MATCH_TYPE,ACCOUNT_TYPE,CONTRIBUTOR_TYPE,OWNERSHIP_IND,DATE_REPORTED,ACCOUNT_STATUS,DISBURSED_DATE,CURRENT_BALANCE,OVERDUE_AMOUNT, 
                CASE 
                        WHEN DAYS_PAST_DUE_HISTORY_MONTH_1 IN ('XXX', 'DDD') 
                        THEN '000' ELSE DAYS_PAST_DUE_HISTORY_MONTH_1 
                    END AS DAYS_PAST_DUE_HISTORY_MONTH_1,
                CASE
                    WHEN FLOOR(DATE_DIFF(CURRENT_DATE(), DISBURSED_DATE, MONTH)) < 2 AND DAYS_PAST_DUE_HISTORY_MONTH_2 IN ('XXX', 'DDD', '') THEN '000'
                    ELSE
                    CASE
                        WHEN DAYS_PAST_DUE_HISTORY_MONTH_2 = '' THEN IFNULL(NULLIF(TRIM(DAYS_PAST_DUE_HISTORY_MONTH_2), ''), '000')
                        ELSE DAYS_PAST_DUE_HISTORY_MONTH_2
                    END
                END AS DAYS_PAST_DUE_HISTORY_MONTH_2,    
                CASE
                    WHEN FLOOR(DATE_DIFF(CURRENT_DATE(), DISBURSED_DATE, MONTH)) < 3 AND DAYS_PAST_DUE_HISTORY_MONTH_3 IN ('XXX', 'DDD', '') THEN '000'
                    ELSE
                    CASE
                        WHEN DAYS_PAST_DUE_HISTORY_MONTH_3 = '' THEN IFNULL(NULLIF(TRIM(DAYS_PAST_DUE_HISTORY_MONTH_3), ''), '000')
                        ELSE DAYS_PAST_DUE_HISTORY_MONTH_3
                    END
                END AS DAYS_PAST_DUE_HISTORY_MONTH_3,
                CASE
                    WHEN FLOOR(DATE_DIFF(CURRENT_DATE(), DISBURSED_DATE, MONTH)) < 4 AND DAYS_PAST_DUE_HISTORY_MONTH_4 IN ('XXX', 'DDD', '') THEN '000'
                    ELSE
                    CASE
                        WHEN DAYS_PAST_DUE_HISTORY_MONTH_4 = '' THEN IFNULL(NULLIF(TRIM(DAYS_PAST_DUE_HISTORY_MONTH_4), ''), '000')
                        ELSE DAYS_PAST_DUE_HISTORY_MONTH_4
                    END
                END AS DAYS_PAST_DUE_HISTORY_MONTH_4,
                CASE
                    WHEN FLOOR(DATE_DIFF(CURRENT_DATE(), DISBURSED_DATE, MONTH)) < 5 AND DAYS_PAST_DUE_HISTORY_MONTH_5 IN ('XXX', 'DDD', '') THEN '000'
                    ELSE
                    CASE
                        WHEN DAYS_PAST_DUE_HISTORY_MONTH_5 = '' THEN IFNULL(NULLIF(TRIM(DAYS_PAST_DUE_HISTORY_MONTH_5), ''), '000')
                        ELSE DAYS_PAST_DUE_HISTORY_MONTH_5
                    END
                END AS DAYS_PAST_DUE_HISTORY_MONTH_5,
                CASE
                    WHEN FLOOR(DATE_DIFF(CURRENT_DATE(), DISBURSED_DATE, MONTH)) < 6 AND DAYS_PAST_DUE_HISTORY_MONTH_6 IN ('XXX', 'DDD', '') THEN '000'
                    ELSE
                    CASE
                        WHEN DAYS_PAST_DUE_HISTORY_MONTH_6 = '' THEN IFNULL(NULLIF(TRIM(DAYS_PAST_DUE_HISTORY_MONTH_6), ''), '000')
                        ELSE DAYS_PAST_DUE_HISTORY_MONTH_6
                    END
                END AS DAYS_PAST_DUE_HISTORY_MONTH_6,
                CASE
                    WHEN FLOOR(DATE_DIFF(CURRENT_DATE(), DISBURSED_DATE, MONTH)) < 7 AND DAYS_PAST_DUE_HISTORY_MONTH_7 IN ('XXX', 'DDD', '') THEN '000'
                    ELSE
                    CASE
                        WHEN DAYS_PAST_DUE_HISTORY_MONTH_7 = '' THEN IFNULL(NULLIF(TRIM(DAYS_PAST_DUE_HISTORY_MONTH_7), ''), '000')
                        ELSE DAYS_PAST_DUE_HISTORY_MONTH_7
                    END
                END AS DAYS_PAST_DUE_HISTORY_MONTH_7,
                CASE
                    WHEN FLOOR(DATE_DIFF(CURRENT_DATE(), DISBURSED_DATE, MONTH)) < 8 AND DAYS_PAST_DUE_HISTORY_MONTH_8 IN ('XXX', 'DDD', '') THEN '000'
                    ELSE
                    CASE
                        WHEN DAYS_PAST_DUE_HISTORY_MONTH_8 = '' THEN IFNULL(NULLIF(TRIM(DAYS_PAST_DUE_HISTORY_MONTH_8), ''), '000')
                        ELSE DAYS_PAST_DUE_HISTORY_MONTH_8
                    END
                END AS DAYS_PAST_DUE_HISTORY_MONTH_8,
                CASE
                    WHEN FLOOR(DATE_DIFF(CURRENT_DATE(), DISBURSED_DATE, MONTH)) < 9 AND DAYS_PAST_DUE_HISTORY_MONTH_9 IN ('XXX', 'DDD', '') THEN '000'
                    ELSE
                    CASE
                        WHEN DAYS_PAST_DUE_HISTORY_MONTH_9 = '' THEN IFNULL(NULLIF(TRIM(DAYS_PAST_DUE_HISTORY_MONTH_9), ''), '000')
                        ELSE DAYS_PAST_DUE_HISTORY_MONTH_9
                    END
                END AS DAYS_PAST_DUE_HISTORY_MONTH_9,
                CASE
                    WHEN FLOOR(DATE_DIFF(CURRENT_DATE(), DISBURSED_DATE, MONTH)) < 10 AND DAYS_PAST_DUE_HISTORY_MONTH_10 IN ('XXX', 'DDD', '') THEN '000'
                    ELSE
                    CASE
                        WHEN DAYS_PAST_DUE_HISTORY_MONTH_10 = '' THEN IFNULL(NULLIF(TRIM(DAYS_PAST_DUE_HISTORY_MONTH_10), ''), '000')
                        ELSE DAYS_PAST_DUE_HISTORY_MONTH_10
                    END
                END AS DAYS_PAST_DUE_HISTORY_MONTH_10,
                CASE
                    WHEN FLOOR(DATE_DIFF(CURRENT_DATE(), DISBURSED_DATE, MONTH)) < 11 AND DAYS_PAST_DUE_HISTORY_MONTH_11 IN ('XXX', 'DDD', '') THEN '000'
                    ELSE
                    CASE
                        WHEN DAYS_PAST_DUE_HISTORY_MONTH_11 = '' THEN IFNULL(NULLIF(TRIM(DAYS_PAST_DUE_HISTORY_MONTH_11), ''), '000')
                        ELSE DAYS_PAST_DUE_HISTORY_MONTH_11
                    END
                END AS DAYS_PAST_DUE_HISTORY_MONTH_11,
                CASE
                    WHEN FLOOR(DATE_DIFF(CURRENT_DATE(), DISBURSED_DATE, MONTH)) < 12 AND DAYS_PAST_DUE_HISTORY_MONTH_12 IN ('XXX', 'DDD', '') THEN '000'
                    ELSE
                    CASE
                        WHEN DAYS_PAST_DUE_HISTORY_MONTH_12 = '' THEN IFNULL(NULLIF(TRIM(DAYS_PAST_DUE_HISTORY_MONTH_12), ''), '000')
                        ELSE DAYS_PAST_DUE_HISTORY_MONTH_12
                    END
                END AS DAYS_PAST_DUE_HISTORY_MONTH_12,
                CASE
                    WHEN FLOOR(DATE_DIFF(CURRENT_DATE(), DISBURSED_DATE, MONTH)) < 13 AND DAYS_PAST_DUE_HISTORY_MONTH_13 IN ('XXX', 'DDD', '') THEN '000'
                    ELSE
                    CASE
                        WHEN DAYS_PAST_DUE_HISTORY_MONTH_13 = '' THEN IFNULL(NULLIF(TRIM(DAYS_PAST_DUE_HISTORY_MONTH_13), ''), '000')
                        ELSE DAYS_PAST_DUE_HISTORY_MONTH_13
                    END
                END AS DAYS_PAST_DUE_HISTORY_MONTH_13,
                CASE
                    WHEN FLOOR(DATE_DIFF(CURRENT_DATE(), DISBURSED_DATE, MONTH)) < 14 AND DAYS_PAST_DUE_HISTORY_MONTH_14 IN ('XXX', 'DDD', '') THEN '000'
                    ELSE
                    CASE
                        WHEN DAYS_PAST_DUE_HISTORY_MONTH_14 = '' THEN IFNULL(NULLIF(TRIM(DAYS_PAST_DUE_HISTORY_MONTH_14), ''), '000')
                        ELSE DAYS_PAST_DUE_HISTORY_MONTH_14
                    END
                END AS DAYS_PAST_DUE_HISTORY_MONTH_14,
                CASE
                    WHEN FLOOR(DATE_DIFF(CURRENT_DATE(), DISBURSED_DATE, MONTH)) < 15 AND DAYS_PAST_DUE_HISTORY_MONTH_15 IN ('XXX', 'DDD', '') THEN '000'
                    ELSE
                    CASE
                        WHEN DAYS_PAST_DUE_HISTORY_MONTH_15 = '' THEN IFNULL(NULLIF(TRIM(DAYS_PAST_DUE_HISTORY_MONTH_15), ''), '000')
                        ELSE DAYS_PAST_DUE_HISTORY_MONTH_15
                    END
                END AS DAYS_PAST_DUE_HISTORY_MONTH_15,
                CASE
                    WHEN FLOOR(DATE_DIFF(CURRENT_DATE(), DISBURSED_DATE, MONTH)) < 16 AND DAYS_PAST_DUE_HISTORY_MONTH_16 IN ('XXX', 'DDD', '') THEN '000'
                    ELSE
                    CASE
                        WHEN DAYS_PAST_DUE_HISTORY_MONTH_16 = '' THEN IFNULL(NULLIF(TRIM(DAYS_PAST_DUE_HISTORY_MONTH_16), ''), '000')
                        ELSE DAYS_PAST_DUE_HISTORY_MONTH_16
                    END
                END AS DAYS_PAST_DUE_HISTORY_MONTH_16,
                CASE
                    WHEN FLOOR(DATE_DIFF(CURRENT_DATE(), DISBURSED_DATE, MONTH)) < 17 AND DAYS_PAST_DUE_HISTORY_MONTH_17 IN ('XXX', 'DDD', '') THEN '000'
                    ELSE
                    CASE
                        WHEN DAYS_PAST_DUE_HISTORY_MONTH_17 = '' THEN IFNULL(NULLIF(TRIM(DAYS_PAST_DUE_HISTORY_MONTH_17), ''), '000')
                        ELSE DAYS_PAST_DUE_HISTORY_MONTH_17
                    END
                END AS DAYS_PAST_DUE_HISTORY_MONTH_17,
                CASE
                    WHEN FLOOR(DATE_DIFF(CURRENT_DATE(), DISBURSED_DATE, MONTH)) < 18 AND DAYS_PAST_DUE_HISTORY_MONTH_18 IN ('XXX', 'DDD', '') THEN '000'
                    ELSE
                    CASE
                        WHEN DAYS_PAST_DUE_HISTORY_MONTH_18 = '' THEN IFNULL(NULLIF(TRIM(DAYS_PAST_DUE_HISTORY_MONTH_18), ''), '000')
                        ELSE DAYS_PAST_DUE_HISTORY_MONTH_18
                    END
                END AS DAYS_PAST_DUE_HISTORY_MONTH_18,
                CASE
                    WHEN FLOOR(DATE_DIFF(CURRENT_DATE(), DISBURSED_DATE, MONTH)) < 19 AND DAYS_PAST_DUE_HISTORY_MONTH_19 IN ('XXX', 'DDD', '') THEN '000'
                    ELSE
                    CASE
                        WHEN DAYS_PAST_DUE_HISTORY_MONTH_19 = '' THEN IFNULL(NULLIF(TRIM(DAYS_PAST_DUE_HISTORY_MONTH_19), ''), '000')
                        ELSE DAYS_PAST_DUE_HISTORY_MONTH_19
                    END
                END AS DAYS_PAST_DUE_HISTORY_MONTH_19,
                CASE
                    WHEN FLOOR(DATE_DIFF(CURRENT_DATE(), DISBURSED_DATE, MONTH)) < 20 AND DAYS_PAST_DUE_HISTORY_MONTH_20 IN ('XXX', 'DDD', '') THEN '000'
                    ELSE
                    CASE
                        WHEN DAYS_PAST_DUE_HISTORY_MONTH_20 = '' THEN IFNULL(NULLIF(TRIM(DAYS_PAST_DUE_HISTORY_MONTH_20), ''), '000')
                        ELSE DAYS_PAST_DUE_HISTORY_MONTH_20
                    END
                END AS DAYS_PAST_DUE_HISTORY_MONTH_20,                  
                CASE
                    WHEN FLOOR(DATE_DIFF(CURRENT_DATE(), DISBURSED_DATE, MONTH)) < 21 AND DAYS_PAST_DUE_HISTORY_MONTH_21 IN ('XXX', 'DDD', '') THEN '000'
                    ELSE
                    CASE
                        WHEN DAYS_PAST_DUE_HISTORY_MONTH_21 = '' THEN IFNULL(NULLIF(TRIM(DAYS_PAST_DUE_HISTORY_MONTH_21), ''), '000')
                        ELSE DAYS_PAST_DUE_HISTORY_MONTH_21
                    END
                END AS DAYS_PAST_DUE_HISTORY_MONTH_21,
                CASE
                    WHEN FLOOR(DATE_DIFF(CURRENT_DATE(), DISBURSED_DATE, MONTH)) < 22 AND DAYS_PAST_DUE_HISTORY_MONTH_22 IN ('XXX', 'DDD', '') THEN '000'
                    ELSE
                    CASE
                        WHEN DAYS_PAST_DUE_HISTORY_MONTH_22 = '' THEN IFNULL(NULLIF(TRIM(DAYS_PAST_DUE_HISTORY_MONTH_22), ''), '000')
                        ELSE DAYS_PAST_DUE_HISTORY_MONTH_22
                    END
                END AS DAYS_PAST_DUE_HISTORY_MONTH_22,
                CASE
                    WHEN FLOOR(DATE_DIFF(CURRENT_DATE(), DISBURSED_DATE, MONTH)) < 23 AND DAYS_PAST_DUE_HISTORY_MONTH_23 IN ('XXX', 'DDD', '') THEN '000'
                    ELSE
                    CASE
                        WHEN DAYS_PAST_DUE_HISTORY_MONTH_23 = '' THEN IFNULL(NULLIF(TRIM(DAYS_PAST_DUE_HISTORY_MONTH_23), ''), '000')
                        ELSE DAYS_PAST_DUE_HISTORY_MONTH_23
                    END
                END AS DAYS_PAST_DUE_HISTORY_MONTH_23,
                CASE
                    WHEN FLOOR(DATE_DIFF(CURRENT_DATE(), DISBURSED_DATE, MONTH)) < 24 AND DAYS_PAST_DUE_HISTORY_MONTH_24 IN ('XXX', 'DDD', '') THEN '000'
                    ELSE
                    CASE
                        WHEN DAYS_PAST_DUE_HISTORY_MONTH_24 = '' THEN IFNULL(NULLIF(TRIM(DAYS_PAST_DUE_HISTORY_MONTH_24), ''), '000')
                        ELSE DAYS_PAST_DUE_HISTORY_MONTH_24
                    END
                END AS DAYS_PAST_DUE_HISTORY_MONTH_24,
                CASE
                    WHEN FLOOR(DATE_DIFF(CURRENT_DATE(), DISBURSED_DATE, MONTH)) < 25 AND DAYS_PAST_DUE_HISTORY_MONTH_25 IN ('XXX', 'DDD', '') THEN '000'
                    ELSE
                    CASE
                        WHEN DAYS_PAST_DUE_HISTORY_MONTH_25 = '' THEN IFNULL(NULLIF(TRIM(DAYS_PAST_DUE_HISTORY_MONTH_25), ''), '000')
                        ELSE DAYS_PAST_DUE_HISTORY_MONTH_25
                    END
                END AS DAYS_PAST_DUE_HISTORY_MONTH_25,
                CASE
                    WHEN FLOOR(DATE_DIFF(CURRENT_DATE(), DISBURSED_DATE, MONTH)) < 26 AND DAYS_PAST_DUE_HISTORY_MONTH_26 IN ('XXX', 'DDD', '') THEN '000'
                    ELSE
                    CASE
                        WHEN DAYS_PAST_DUE_HISTORY_MONTH_26 = '' THEN IFNULL(NULLIF(TRIM(DAYS_PAST_DUE_HISTORY_MONTH_26), ''), '000')
                        ELSE DAYS_PAST_DUE_HISTORY_MONTH_26
                    END
                END AS DAYS_PAST_DUE_HISTORY_MONTH_26,
                CASE
                    WHEN FLOOR(DATE_DIFF(CURRENT_DATE(), DISBURSED_DATE, MONTH)) < 27 AND DAYS_PAST_DUE_HISTORY_MONTH_27 IN ('XXX', 'DDD', '') THEN '000'
                    ELSE
                    CASE
                        WHEN DAYS_PAST_DUE_HISTORY_MONTH_27 = '' THEN IFNULL(NULLIF(TRIM(DAYS_PAST_DUE_HISTORY_MONTH_27), ''), '000')
                        ELSE DAYS_PAST_DUE_HISTORY_MONTH_27
                    END
                END AS DAYS_PAST_DUE_HISTORY_MONTH_27,
                CASE
                    WHEN FLOOR(DATE_DIFF(CURRENT_DATE(), DISBURSED_DATE, MONTH)) < 28 AND DAYS_PAST_DUE_HISTORY_MONTH_28 IN ('XXX', 'DDD', '') THEN '000'
                    ELSE
                    CASE
                        WHEN DAYS_PAST_DUE_HISTORY_MONTH_28 = '' THEN IFNULL(NULLIF(TRIM(DAYS_PAST_DUE_HISTORY_MONTH_28), ''), '000')
                        ELSE DAYS_PAST_DUE_HISTORY_MONTH_28
                    END
                END AS DAYS_PAST_DUE_HISTORY_MONTH_28,
                CASE
                    WHEN FLOOR(DATE_DIFF(CURRENT_DATE(), DISBURSED_DATE, MONTH)) < 29 AND DAYS_PAST_DUE_HISTORY_MONTH_29 IN ('XXX', 'DDD', '') THEN '000'
                    ELSE
                    CASE
                        WHEN DAYS_PAST_DUE_HISTORY_MONTH_29 = '' THEN IFNULL(NULLIF(TRIM(DAYS_PAST_DUE_HISTORY_MONTH_29), ''), '000')
                        ELSE DAYS_PAST_DUE_HISTORY_MONTH_29
                    END
                END AS DAYS_PAST_DUE_HISTORY_MONTH_29,
                CASE
                    WHEN FLOOR(DATE_DIFF(CURRENT_DATE(), DISBURSED_DATE, MONTH)) < 30 AND DAYS_PAST_DUE_HISTORY_MONTH_30 IN ('XXX', 'DDD', '') THEN '000'
                    ELSE
                    CASE
                        WHEN DAYS_PAST_DUE_HISTORY_MONTH_30 = '' THEN IFNULL(NULLIF(TRIM(DAYS_PAST_DUE_HISTORY_MONTH_30), ''), '000')
                        ELSE DAYS_PAST_DUE_HISTORY_MONTH_30
                    END
                END AS DAYS_PAST_DUE_HISTORY_MONTH_30,                  
                CASE
                    WHEN FLOOR(DATE_DIFF(CURRENT_DATE(), DISBURSED_DATE, MONTH)) < 31 AND DAYS_PAST_DUE_HISTORY_MONTH_31 IN ('XXX', 'DDD', '') THEN '000'
                    ELSE
                    CASE
                        WHEN DAYS_PAST_DUE_HISTORY_MONTH_31 = '' THEN IFNULL(NULLIF(TRIM(DAYS_PAST_DUE_HISTORY_MONTH_31), ''), '000')
                        ELSE DAYS_PAST_DUE_HISTORY_MONTH_31
                    END
                END AS DAYS_PAST_DUE_HISTORY_MONTH_31,
                CASE
                    WHEN FLOOR(DATE_DIFF(CURRENT_DATE(), DISBURSED_DATE, MONTH)) < 32 AND DAYS_PAST_DUE_HISTORY_MONTH_32 IN ('XXX', 'DDD', '') THEN '000'
                    ELSE
                    CASE
                        WHEN DAYS_PAST_DUE_HISTORY_MONTH_32 = '' THEN IFNULL(NULLIF(TRIM(DAYS_PAST_DUE_HISTORY_MONTH_32), ''), '000')
                        ELSE DAYS_PAST_DUE_HISTORY_MONTH_32
                    END
                END AS DAYS_PAST_DUE_HISTORY_MONTH_32,
                CASE
                    WHEN FLOOR(DATE_DIFF(CURRENT_DATE(), DISBURSED_DATE, MONTH)) < 33 AND DAYS_PAST_DUE_HISTORY_MONTH_33 IN ('XXX', 'DDD', '') THEN '000'
                    ELSE
                    CASE
                        WHEN DAYS_PAST_DUE_HISTORY_MONTH_33 = '' THEN IFNULL(NULLIF(TRIM(DAYS_PAST_DUE_HISTORY_MONTH_33), ''), '000')
                        ELSE DAYS_PAST_DUE_HISTORY_MONTH_33
                    END
                END AS DAYS_PAST_DUE_HISTORY_MONTH_33,
                CASE
                    WHEN FLOOR(DATE_DIFF(CURRENT_DATE(), DISBURSED_DATE, MONTH)) < 34 AND DAYS_PAST_DUE_HISTORY_MONTH_34 IN ('XXX', 'DDD', '') THEN '000'
                    ELSE
                    CASE
                        WHEN DAYS_PAST_DUE_HISTORY_MONTH_34 = '' THEN IFNULL(NULLIF(TRIM(DAYS_PAST_DUE_HISTORY_MONTH_34), ''), '000')
                        ELSE DAYS_PAST_DUE_HISTORY_MONTH_34
                    END
                END AS DAYS_PAST_DUE_HISTORY_MONTH_34,
                CASE
                    WHEN FLOOR(DATE_DIFF(CURRENT_DATE(), DISBURSED_DATE, MONTH)) < 35 AND DAYS_PAST_DUE_HISTORY_MONTH_35 IN ('XXX', 'DDD', '') THEN '000'
                    ELSE
                    CASE
                        WHEN DAYS_PAST_DUE_HISTORY_MONTH_35 = '' THEN IFNULL(NULLIF(TRIM(DAYS_PAST_DUE_HISTORY_MONTH_35), ''), '000')
                        ELSE DAYS_PAST_DUE_HISTORY_MONTH_35
                    END
                END AS DAYS_PAST_DUE_HISTORY_MONTH_35,
                CASE
                    WHEN FLOOR(DATE_DIFF(CURRENT_DATE(), DISBURSED_DATE, MONTH)) < 36 AND DAYS_PAST_DUE_HISTORY_MONTH_36 IN ('XXX', 'DDD', '') THEN '000'
                    ELSE
                    CASE
                        WHEN DAYS_PAST_DUE_HISTORY_MONTH_36 = '' THEN IFNULL(NULLIF(TRIM(DAYS_PAST_DUE_HISTORY_MONTH_36), ''), '000')
                        ELSE DAYS_PAST_DUE_HISTORY_MONTH_36
                    END
                END AS DAYS_PAST_DUE_HISTORY_MONTH_36,
                COALESCE(AMOUNT_OVERDUE_HISTORY_MONTH_1, 0.0) AS AMOUNT_OVERDUE_HISTORY_MONTH_1,
                COALESCE(AMOUNT_OVERDUE_HISTORY_MONTH_2, 0.0) AS AMOUNT_OVERDUE_HISTORY_MONTH_2,
                COALESCE(AMOUNT_OVERDUE_HISTORY_MONTH_3, 0.0) AS AMOUNT_OVERDUE_HISTORY_MONTH_3,
                COALESCE(AMOUNT_OVERDUE_HISTORY_MONTH_4, 0.0) AS AMOUNT_OVERDUE_HISTORY_MONTH_4,
                COALESCE(AMOUNT_OVERDUE_HISTORY_MONTH_5, 0.0) AS AMOUNT_OVERDUE_HISTORY_MONTH_5,
                COALESCE(AMOUNT_OVERDUE_HISTORY_MONTH_6, 0.0) AS AMOUNT_OVERDUE_HISTORY_MONTH_6,
                COALESCE(AMOUNT_OVERDUE_HISTORY_MONTH_7, 0.0) AS AMOUNT_OVERDUE_HISTORY_MONTH_7,
                COALESCE(AMOUNT_OVERDUE_HISTORY_MONTH_8, 0.0) AS AMOUNT_OVERDUE_HISTORY_MONTH_8,
                COALESCE(AMOUNT_OVERDUE_HISTORY_MONTH_9, 0.0) AS AMOUNT_OVERDUE_HISTORY_MONTH_9,
                COALESCE(AMOUNT_OVERDUE_HISTORY_MONTH_10, 0.0) AS AMOUNT_OVERDUE_HISTORY_MONTH_10,
                COALESCE(AMOUNT_OVERDUE_HISTORY_MONTH_11, 0.0) AS AMOUNT_OVERDUE_HISTORY_MONTH_11,
                COALESCE(AMOUNT_OVERDUE_HISTORY_MONTH_12, 0.0) AS AMOUNT_OVERDUE_HISTORY_MONTH_12,
                COALESCE(AMOUNT_OVERDUE_HISTORY_MONTH_13, 0.0) AS AMOUNT_OVERDUE_HISTORY_MONTH_13,
                COALESCE(AMOUNT_OVERDUE_HISTORY_MONTH_14, 0.0) AS AMOUNT_OVERDUE_HISTORY_MONTH_14,
                COALESCE(AMOUNT_OVERDUE_HISTORY_MONTH_15, 0.0) AS AMOUNT_OVERDUE_HISTORY_MONTH_15,
                COALESCE(AMOUNT_OVERDUE_HISTORY_MONTH_16, 0.0) AS AMOUNT_OVERDUE_HISTORY_MONTH_16,
                COALESCE(AMOUNT_OVERDUE_HISTORY_MONTH_17, 0.0) AS AMOUNT_OVERDUE_HISTORY_MONTH_17,
                COALESCE(AMOUNT_OVERDUE_HISTORY_MONTH_18, 0.0) AS AMOUNT_OVERDUE_HISTORY_MONTH_18,
                COALESCE(AMOUNT_OVERDUE_HISTORY_MONTH_19, 0.0) AS AMOUNT_OVERDUE_HISTORY_MONTH_19,
                COALESCE(AMOUNT_OVERDUE_HISTORY_MONTH_20, 0.0) AS AMOUNT_OVERDUE_HISTORY_MONTH_20,
                COALESCE(AMOUNT_OVERDUE_HISTORY_MONTH_21, 0.0) AS AMOUNT_OVERDUE_HISTORY_MONTH_21,
                COALESCE(AMOUNT_OVERDUE_HISTORY_MONTH_22, 0.0) AS AMOUNT_OVERDUE_HISTORY_MONTH_22,
                COALESCE(AMOUNT_OVERDUE_HISTORY_MONTH_23, 0.0) AS AMOUNT_OVERDUE_HISTORY_MONTH_23,
                COALESCE(AMOUNT_OVERDUE_HISTORY_MONTH_24, 0.0) AS AMOUNT_OVERDUE_HISTORY_MONTH_24,
                COALESCE(AMOUNT_OVERDUE_HISTORY_MONTH_25, 0.0) AS AMOUNT_OVERDUE_HISTORY_MONTH_25,
                COALESCE(AMOUNT_OVERDUE_HISTORY_MONTH_26, 0.0) AS AMOUNT_OVERDUE_HISTORY_MONTH_26,
                COALESCE(AMOUNT_OVERDUE_HISTORY_MONTH_27, 0.0) AS AMOUNT_OVERDUE_HISTORY_MONTH_27,
                COALESCE(AMOUNT_OVERDUE_HISTORY_MONTH_28, 0.0) AS AMOUNT_OVERDUE_HISTORY_MONTH_28,
                COALESCE(AMOUNT_OVERDUE_HISTORY_MONTH_29, 0.0) AS AMOUNT_OVERDUE_HISTORY_MONTH_29,
                COALESCE(AMOUNT_OVERDUE_HISTORY_MONTH_30, 0.0) AS AMOUNT_OVERDUE_HISTORY_MONTH_30,
                COALESCE(AMOUNT_OVERDUE_HISTORY_MONTH_31, 0.0) AS AMOUNT_OVERDUE_HISTORY_MONTH_31,
                COALESCE(AMOUNT_OVERDUE_HISTORY_MONTH_32, 0.0) AS AMOUNT_OVERDUE_HISTORY_MONTH_32,
                COALESCE(AMOUNT_OVERDUE_HISTORY_MONTH_33, 0.0) AS AMOUNT_OVERDUE_HISTORY_MONTH_33,
                COALESCE(AMOUNT_OVERDUE_HISTORY_MONTH_34, 0.0) AS AMOUNT_OVERDUE_HISTORY_MONTH_34,
                COALESCE(AMOUNT_OVERDUE_HISTORY_MONTH_35, 0.0) AS AMOUNT_OVERDUE_HISTORY_MONTH_35,
                COALESCE(AMOUNT_OVERDUE_HISTORY_MONTH_36, 0.0) AS AMOUNT_OVERDUE_HISTORY_MONTH_36,
                INCOME,AMOUNT,AGE_COHORT,STATE
                FROM `{PROJECT_ID}.{DATASET_ID}.{table_id}`
                WHERE PRIMARY_NO_OF_ACCOUNTS < 100
                )
                SELECT * FROM RankedCreditReports
                WHERE NOT (
                    (DAYS_PAST_DUE_HISTORY_MONTH_1 = 'XXX' OR DAYS_PAST_DUE_HISTORY_MONTH_1 = 'DDD') AND
                    (DAYS_PAST_DUE_HISTORY_MONTH_2 = 'XXX' OR DAYS_PAST_DUE_HISTORY_MONTH_2 = 'DDD') AND
                    (DAYS_PAST_DUE_HISTORY_MONTH_3 = 'XXX' OR DAYS_PAST_DUE_HISTORY_MONTH_3 = 'DDD') AND
                    (DAYS_PAST_DUE_HISTORY_MONTH_4 = 'XXX' OR DAYS_PAST_DUE_HISTORY_MONTH_4 = 'DDD') AND
                    (DAYS_PAST_DUE_HISTORY_MONTH_5 = 'XXX' OR DAYS_PAST_DUE_HISTORY_MONTH_5 = 'DDD') AND
                    (DAYS_PAST_DUE_HISTORY_MONTH_6 = 'XXX' OR DAYS_PAST_DUE_HISTORY_MONTH_6 = 'DDD') AND
                    (DAYS_PAST_DUE_HISTORY_MONTH_7 = 'XXX' OR DAYS_PAST_DUE_HISTORY_MONTH_7 = 'DDD') AND
                    (DAYS_PAST_DUE_HISTORY_MONTH_8 = 'XXX' OR DAYS_PAST_DUE_HISTORY_MONTH_8 = 'DDD') AND
                    (DAYS_PAST_DUE_HISTORY_MONTH_9 = 'XXX' OR DAYS_PAST_DUE_HISTORY_MONTH_9 = 'DDD') AND
                    (DAYS_PAST_DUE_HISTORY_MONTH_10 = 'XXX' OR DAYS_PAST_DUE_HISTORY_MONTH_10 = 'DDD') AND
                    (DAYS_PAST_DUE_HISTORY_MONTH_11 = 'XXX' OR DAYS_PAST_DUE_HISTORY_MONTH_11 = 'DDD') AND
                    (DAYS_PAST_DUE_HISTORY_MONTH_12 = 'XXX' OR DAYS_PAST_DUE_HISTORY_MONTH_12 = 'DDD') AND
                    (DAYS_PAST_DUE_HISTORY_MONTH_13 = 'XXX' OR DAYS_PAST_DUE_HISTORY_MONTH_13 = 'DDD') AND
                    (DAYS_PAST_DUE_HISTORY_MONTH_14 = 'XXX' OR DAYS_PAST_DUE_HISTORY_MONTH_14 = 'DDD') AND
                    (DAYS_PAST_DUE_HISTORY_MONTH_15 = 'XXX' OR DAYS_PAST_DUE_HISTORY_MONTH_15 = 'DDD') AND
                    (DAYS_PAST_DUE_HISTORY_MONTH_16 = 'XXX' OR DAYS_PAST_DUE_HISTORY_MONTH_16 = 'DDD') AND
                    (DAYS_PAST_DUE_HISTORY_MONTH_17 = 'XXX' OR DAYS_PAST_DUE_HISTORY_MONTH_17 = 'DDD') AND
                    (DAYS_PAST_DUE_HISTORY_MONTH_18 = 'XXX' OR DAYS_PAST_DUE_HISTORY_MONTH_18 = 'DDD') AND
                    (DAYS_PAST_DUE_HISTORY_MONTH_19 = 'XXX' OR DAYS_PAST_DUE_HISTORY_MONTH_19 = 'DDD') AND
                    (DAYS_PAST_DUE_HISTORY_MONTH_20 = 'XXX' OR DAYS_PAST_DUE_HISTORY_MONTH_20 = 'DDD') AND
                    (DAYS_PAST_DUE_HISTORY_MONTH_21 = 'XXX' OR DAYS_PAST_DUE_HISTORY_MONTH_21 = 'DDD') AND
                    (DAYS_PAST_DUE_HISTORY_MONTH_22 = 'XXX' OR DAYS_PAST_DUE_HISTORY_MONTH_22 = 'DDD') AND
                    (DAYS_PAST_DUE_HISTORY_MONTH_23 = 'XXX' OR DAYS_PAST_DUE_HISTORY_MONTH_23 = 'DDD') AND
                    (DAYS_PAST_DUE_HISTORY_MONTH_24 = 'XXX' OR DAYS_PAST_DUE_HISTORY_MONTH_24 = 'DDD') AND
                    (DAYS_PAST_DUE_HISTORY_MONTH_25 = 'XXX' OR DAYS_PAST_DUE_HISTORY_MONTH_25 = 'DDD') AND
                    (DAYS_PAST_DUE_HISTORY_MONTH_26 = 'XXX' OR DAYS_PAST_DUE_HISTORY_MONTH_26 = 'DDD') AND
                    (DAYS_PAST_DUE_HISTORY_MONTH_27 = 'XXX' OR DAYS_PAST_DUE_HISTORY_MONTH_27 = 'DDD') AND
                    (DAYS_PAST_DUE_HISTORY_MONTH_28 = 'XXX' OR DAYS_PAST_DUE_HISTORY_MONTH_28 = 'DDD') AND
                    (DAYS_PAST_DUE_HISTORY_MONTH_29 = 'XXX' OR DAYS_PAST_DUE_HISTORY_MONTH_29 = 'DDD') AND
                    (DAYS_PAST_DUE_HISTORY_MONTH_30 = 'XXX' OR DAYS_PAST_DUE_HISTORY_MONTH_30 = 'DDD') AND
                    (DAYS_PAST_DUE_HISTORY_MONTH_31 = 'XXX' OR DAYS_PAST_DUE_HISTORY_MONTH_31 = 'DDD') AND
                    (DAYS_PAST_DUE_HISTORY_MONTH_32 = 'XXX' OR DAYS_PAST_DUE_HISTORY_MONTH_32 = 'DDD') AND
                    (DAYS_PAST_DUE_HISTORY_MONTH_33 = 'XXX' OR DAYS_PAST_DUE_HISTORY_MONTH_33 = 'DDD') AND
                    (DAYS_PAST_DUE_HISTORY_MONTH_34 = 'XXX' OR DAYS_PAST_DUE_HISTORY_MONTH_34 = 'DDD') AND
                    (DAYS_PAST_DUE_HISTORY_MONTH_35 = 'XXX' OR DAYS_PAST_DUE_HISTORY_MONTH_35 = 'DDD') AND
                    (DAYS_PAST_DUE_HISTORY_MONTH_36 = 'XXX' OR DAYS_PAST_DUE_HISTORY_MONTH_36 = 'DDD') 
)'''

    #print(query)
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
    #Cell 1
    query =  f'''SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.{table_id}`;'''
    # Run the query
    print(PROJECT_ID)
    print(DATASET_ID)
    print(table_id)
    print(query)
    query_job = client.query(query)
    results = query_job.result()
    #Mama
    #df = pd.DataFrame(data=[list(row.values()) for row in results],
    #                  columns=[field.name for field in query_job.schema])
    
    # Convert the query results into a DataFrame
    #Mami
    df = results.to_dataframe()

    #Cell 2
    # Replace XXX and DDD values from the dataframe
    def process_dpd_column(column):
        processed_values = []
        for value in column:
            value = int(value) if value.isdigit() else 0
            value = max(value - 30, 0)
            processed_values.append(value)
        return processed_values

    for col in df.columns:
        if col.startswith('DAYS_PAST_DUE_HISTORY_MONTH_'):
            df[col] = process_dpd_column(df[col])
    

    dpd_col = [f'DAYS_PAST_DUE_HISTORY_MONTH_{i}' for i in range(1, 37)]
    dpd_df = df[dpd_col].copy()

    amt_col = [f'AMOUNT_OVERDUE_HISTORY_MONTH_{i}' for i in range(1, 37)]
    amt_df = df[amt_col].copy()
    m_df = pd.concat([dpd_df, amt_df], axis = 1)

    dpd_df = None
    amt_df = None

    # Initialize a column for customer categorization
    m_df['ROOPYA_ACCOUNT_STATUS'] = 'Uncategorised'

    # Condition 1: Last 36 months all DPD is 0
    condition_1 = m_df.iloc[:, 1:37].isin([0]).all(axis=1)  

    m_df.loc[condition_1, 'ROOPYA_ACCOUNT_STATUS'] = 'Good'

    # Condition 2: Last 6 months DPD 0 and rest DPD amount < 5000
    condition_2 = ((m_df.iloc[:, 1:7].isin([0]).all(axis=1)) &
                    (m_df.iloc[:, 43:73].applymap(lambda x: float(x) if str(x).isdigit() else 0).sum(axis=1) < 5000))

    m_df.loc[condition_2, 'ROOPYA_ACCOUNT_STATUS'] = 'Good'

    # Condition 3: Last 6 months any DPD is more than 0
    condition_3 = (~m_df.iloc[:, 1:7].isin([0]).all(axis=1))

    m_df.loc[condition_3, 'ROOPYA_ACCOUNT_STATUS'] = 'Bad'

    # Condition 4: Last 36 months any DPD > 5000
    condition_4 = (m_df.iloc[:, 37:73].applymap(lambda x: float(x) if str(x).isdigit() else 0) > 5000).any(axis=1)

    m_df.loc[condition_4, 'ROOPYA_ACCOUNT_STATUS'] = 'Bad'

    cs_col = ['ROOPYA_ACCOUNT_STATUS']
    cs_df = m_df[cs_col].copy()

    m_df = None

    final_df = pd.concat([df, cs_df], axis = 1)
    #df = None
    cs_df = None

    # List of columns to remove
    columns_to_remove = ['SELF_INDICATOR', 'MATCH_TYPE', 'DISBURSED_DATE', 'DATE_REPORTED', 'DAYS_PAST_DUE_HISTORY_MONTH_1', 'DAYS_PAST_DUE_HISTORY_MONTH_2','DAYS_PAST_DUE_HISTORY_MONTH_3', 'DAYS_PAST_DUE_HISTORY_MONTH_4','DAYS_PAST_DUE_HISTORY_MONTH_5', 'DAYS_PAST_DUE_HISTORY_MONTH_6','DAYS_PAST_DUE_HISTORY_MONTH_7', 'DAYS_PAST_DUE_HISTORY_MONTH_8','DAYS_PAST_DUE_HISTORY_MONTH_9', 'DAYS_PAST_DUE_HISTORY_MONTH_10','DAYS_PAST_DUE_HISTORY_MONTH_11', 'DAYS_PAST_DUE_HISTORY_MONTH_12','DAYS_PAST_DUE_HISTORY_MONTH_13', 'DAYS_PAST_DUE_HISTORY_MONTH_14','DAYS_PAST_DUE_HISTORY_MONTH_15', 'DAYS_PAST_DUE_HISTORY_MONTH_16','DAYS_PAST_DUE_HISTORY_MONTH_17', 'DAYS_PAST_DUE_HISTORY_MONTH_18','DAYS_PAST_DUE_HISTORY_MONTH_19', 'DAYS_PAST_DUE_HISTORY_MONTH_20', 'DAYS_PAST_DUE_HISTORY_MONTH_21', 'DAYS_PAST_DUE_HISTORY_MONTH_22','DAYS_PAST_DUE_HISTORY_MONTH_23', 'DAYS_PAST_DUE_HISTORY_MONTH_24', 'DAYS_PAST_DUE_HISTORY_MONTH_25', 'DAYS_PAST_DUE_HISTORY_MONTH_26','DAYS_PAST_DUE_HISTORY_MONTH_27', 'DAYS_PAST_DUE_HISTORY_MONTH_28','DAYS_PAST_DUE_HISTORY_MONTH_29', 'DAYS_PAST_DUE_HISTORY_MONTH_30','DAYS_PAST_DUE_HISTORY_MONTH_31', 'DAYS_PAST_DUE_HISTORY_MONTH_32','DAYS_PAST_DUE_HISTORY_MONTH_33', 'DAYS_PAST_DUE_HISTORY_MONTH_34','DAYS_PAST_DUE_HISTORY_MONTH_35', 'DAYS_PAST_DUE_HISTORY_MONTH_36','AMOUNT_OVERDUE_HISTORY_MONTH_1', 'AMOUNT_OVERDUE_HISTORY_MONTH_2','AMOUNT_OVERDUE_HISTORY_MONTH_3', 'AMOUNT_OVERDUE_HISTORY_MONTH_4','AMOUNT_OVERDUE_HISTORY_MONTH_5', 'AMOUNT_OVERDUE_HISTORY_MONTH_6','AMOUNT_OVERDUE_HISTORY_MONTH_7', 'AMOUNT_OVERDUE_HISTORY_MONTH_8','AMOUNT_OVERDUE_HISTORY_MONTH_9', 'AMOUNT_OVERDUE_HISTORY_MONTH_10','AMOUNT_OVERDUE_HISTORY_MONTH_11', 'AMOUNT_OVERDUE_HISTORY_MONTH_12','AMOUNT_OVERDUE_HISTORY_MONTH_13', 'AMOUNT_OVERDUE_HISTORY_MONTH_14','AMOUNT_OVERDUE_HISTORY_MONTH_15', 'AMOUNT_OVERDUE_HISTORY_MONTH_16','AMOUNT_OVERDUE_HISTORY_MONTH_17', 'AMOUNT_OVERDUE_HISTORY_MONTH_18','AMOUNT_OVERDUE_HISTORY_MONTH_19', 'AMOUNT_OVERDUE_HISTORY_MONTH_20','AMOUNT_OVERDUE_HISTORY_MONTH_21', 'AMOUNT_OVERDUE_HISTORY_MONTH_22','AMOUNT_OVERDUE_HISTORY_MONTH_23', 'AMOUNT_OVERDUE_HISTORY_MONTH_24','AMOUNT_OVERDUE_HISTORY_MONTH_25', 'AMOUNT_OVERDUE_HISTORY_MONTH_26','AMOUNT_OVERDUE_HISTORY_MONTH_27', 'AMOUNT_OVERDUE_HISTORY_MONTH_28','AMOUNT_OVERDUE_HISTORY_MONTH_29', 'AMOUNT_OVERDUE_HISTORY_MONTH_30','AMOUNT_OVERDUE_HISTORY_MONTH_31', 'AMOUNT_OVERDUE_HISTORY_MONTH_32','AMOUNT_OVERDUE_HISTORY_MONTH_33', 'AMOUNT_OVERDUE_HISTORY_MONTH_34','AMOUNT_OVERDUE_HISTORY_MONTH_35', 'AMOUNT_OVERDUE_HISTORY_MONTH_36', 'AMOUNT']

    # Remove the specified columns
    final_df_cleaned = final_df.drop(columns=columns_to_remove)

    final_df = final_df_cleaned

    final_df.shape

    final_df["CURRENT_BALANCE"].isnull().sum()

    # List of columns to replace
    columns_to_process = ['CURRENT_BALANCE', 'INCOME', 'OVERDUE_AMOUNT']

    # Replace null by 0 
    final_df[columns_to_process] = final_df[columns_to_process].fillna(0)

    final_df1 = final_df[(final_df['CURRENT_BALANCE'] >= 0) & (final_df['CURRENT_BALANCE'] <= 10000000) & (final_df['PRIMARY_NO_OF_ACCOUNTS'] <= 100)]

    df = final_df1

    pd.set_option('display.float_format', '{:.2f}'.format)
    df.describe()

    df1 = df[(df['PRIMARY_CURRENT_BALANCE'] >= 0) & (df['PRIMARY_CURRENT_BALANCE'] <= 10000000) & (df['PRIMARY_SANCTIONED_AMOUNT'] >= 0) & (df['PRIMARY_SANCTIONED_AMOUNT'] <= 10000000) & (df['PRIMARY_DISTRIBUTED_AMOUNT'] >= 0) & (df['PRIMARY_DISTRIBUTED_AMOUNT'] <= 10000000) & (df['PRIMARY_INSTALLMENT_AMOUNT'] >= 0) & (df['PRIMARY_INSTALLMENT_AMOUNT'] <= 10000000) & (df['INCOME'] >= 0) & (df['INCOME'] <= 10000000)]

    pd.set_option('display.float_format', '{:.2f}'.format)
    df1.describe()

    df = df1.copy()

    # Specify the columns to consider for duplicates
    columns_to_check = ['CREDIT_REPORT_ID','PRIMARY_NO_OF_ACCOUNTS', 'PRIMARY_ACTIVE_ACCOUNTS', 'PRIMARY_OVERDUE_ACCOUNTS', 'PRIMARY_CURRENT_BALANCE', 'PRIMARY_SANCTIONED_AMOUNT', 'PRIMARY_DISTRIBUTED_AMOUNT', 'PRIMARY_INSTALLMENT_AMOUNT', 'NEW_ACCOUNTS_IN_LAST_SIX_MONTHS', 'AVERAGE_ACCOUNT_AGE', 'NO_OF_INQUIRIES', 'INCOME', 'AGE_COHORT', 'STATE']

    # Drop duplicates based on the specified columns
    cl_df = df.drop_duplicates(subset='CREDIT_REPORT_ID')

    df.columns[df.isnull().any()]

    # Define loan categories
    loan_categories = {
        'Credit Card': 'Credit Card', 'Loan on Credit Card': 'Credit Card', 'Secured Credit Card': 'Credit Card',
        'Corporate Credit Card': 'Credit Card', 'Kisan Credit Card': 'Credit Card', 'Overdraft': 'Personal Loan',
        'Personal Loan': 'Personal Loan', 'Consumer Loan': 'Personal Loan', 
        'Loan Aganist Bank Deposits': 'Personal Loan', 'OD on Savings Account': 'Personal Loan', 
        'Microfinance Personal Loan': 'Personal Loan', 'Loan to Professional': 'Personal Loan',
        'Auto Loan (Personal)': 'Auto Loan', 'Two Wheeler Loan': 'Auto Loan', 'Used Car Loan': 'Auto Loan',
        'Commercial Vehicle Loan': 'Auto Loan', 'Used Tractor Loan': 'Auto Loan', 
        'Housing Loan': 'Home Loan', 'Property Loan': 'Home Loan', 'Leasing': 'Home Loan',
        'Microfinance Housing Loan': 'Home Loan'
    }

    # Map the loan categories to the 'ACCOUNT_TYPE' column
    df['Loan Category'] = df['ACCOUNT_TYPE'].map(loan_categories)

    # Pivot the DataFrame to create separate columns for each loan category
    acc_df = df.pivot_table(index='CREDIT_REPORT_ID', columns='Loan Category', aggfunc= 'size', fill_value=0)

    # Reset the index to make 'CREDIT_REPORT_ID' a column again
    acc_df.reset_index(inplace=True)

    # Rename columns for better readability
    acc_df.rename(columns={'Credit Card': 'ACCOUNT_TYPE_Credit Card', 'Home loan': 'ACCOUNT_TYPE_Home Loan', 'Auto Loan': 'ACCOUNT_TYPE_Auto Loan', 'Personal Loan': 'ACCOUNT_TYPE_Personal Loan'}, inplace=True)

    merged_df = acc_df.merge(df, on="CREDIT_REPORT_ID", how="left")

    merged_df["CREDIT_REPORT_ID"].duplicated().sum()

    merged_df.columns[merged_df.isnull().any()]

    acty_df = merged_df.drop_duplicates(subset='CREDIT_REPORT_ID')

    # Pivot CONTRIBUTOR_TYPE data
    ct_df = df.pivot_table(index = 'CREDIT_REPORT_ID', columns = 'CONTRIBUTOR_TYPE', aggfunc ='size', fill_value = 0)

    # Reset index to have 'CREDIT_REPORT_ID' as a regular column
    ct_df.reset_index(inplace=True)

    # Rename columns for better readability
    ct_df.rename(columns={'PRB': 'CONTRIBUTOR_TYPE_PRB', 'NBF': 'CONTRIBUTOR_TYPE_NBF'}, inplace=True)

    merged_df = acty_df.merge(ct_df, on="CREDIT_REPORT_ID", how="left")

    # Pivot OWNERSHIP_IND data
    oi_df = df.pivot_table(index = 'CREDIT_REPORT_ID', columns = 'OWNERSHIP_IND', aggfunc = 'size', fill_value = 0)

    # Reset index to have 'CREDIT_REPORT_ID' as a regular column
    oi_df.reset_index(inplace=True)

    # Rename columns for better readability
    oi_df.rename(columns={'Individual': 'OWNERSHIP_IND_Individual', 'Supl Card Holder': 'OWNERSHIP_IND_Supl Card Holder', 'Joint': 'OWNERSHIP_IND_Joint', 'Guarantor': 'OWNERSHIP_IND_Guarantor'}, inplace=True)

    merged1_df = merged_df.merge(oi_df, on="CREDIT_REPORT_ID", how="left")

    # Pivot ACCOUNT_STATUS data
    as_df = df.pivot_table(index='CREDIT_REPORT_ID', columns='ACCOUNT_STATUS', aggfunc='size', fill_value=0)

    # Reset index and remove the name of the columns index
    as_df.reset_index(inplace=True)

    # Rename columns for better readability
    as_df.rename(columns={'Active': 'ACCOUNT_STATUS_Active', 'Closed': 'ACCOUNT_STATUS_Closed'}, inplace=True)

    merged2_df = merged1_df.merge(as_df, on="CREDIT_REPORT_ID", how="left")

    agg_data = df[['CREDIT_REPORT_ID', 'ACCOUNT_STATUS', 'CURRENT_BALANCE', 'OVERDUE_AMOUNT']]
    df_dummy = pd.DataFrame({'CREDIT_REPORT_ID': ['RoopyaDummy', 'RoopyaDummy'], 'ACCOUNT_STATUS': ['Active', 'Closed'], 'CURRENT_BALANCE': [0,0], 'OVERDUE_AMOUNT': [0,0]})
    agg_data = pd.concat([agg_data, df_dummy])
    print(agg_data)

    def aggregation(df):
        # Create a pivot table to aggregate balances and overdue amounts
        pivot_df = df.pivot_table(index='CREDIT_REPORT_ID', columns='ACCOUNT_STATUS', 
                                values=['CURRENT_BALANCE', 'OVERDUE_AMOUNT'], 
                                aggfunc={'CURRENT_BALANCE': 'sum', 'OVERDUE_AMOUNT': 'sum'}, 
                                fill_value=0)
        
        # Flatten the multi-level column index
        pivot_df.columns = [f'{agg}_{status}' for status, agg in pivot_df.columns]
        
        # Reset the index to make 'CREDIT_REPORT_ID' a regular column
        pivot_df.reset_index(inplace=True)
        print("pivot_df",pivot_df.columns)
        
        return pivot_df

    # Call the function to aggregate balances
    aggregated_data = aggregation(agg_data)

    merged3_df = merged2_df.merge(aggregated_data, on="CREDIT_REPORT_ID", how="left")

    merged2_df = merged2_df.drop(['CURRENT_BALANCE', 'OVERDUE_AMOUNT'], axis = 1)

    merged3_df["ROOPYA_ACCOUNT_STATUS"].value_counts()

    cri_df = merged3_df[['CREDIT_REPORT_ID', 'ROOPYA_ACCOUNT_STATUS']]
    print("cri_df",cri_df.columns)

    # Custom function to determine the final status
    def categorize_customer(group):
        unique_statuses = group['ROOPYA_ACCOUNT_STATUS'].unique()

        if len(unique_statuses) == 1:
            return unique_statuses[0]
        elif 'Bad' in unique_statuses:
            return 'Bad'
        else:
            return 'Good'

    result = cri_df.groupby('CREDIT_REPORT_ID').apply(categorize_customer)

    # Create a new DataFrame with the summarized results
    final_status_df = result.reset_index(name='ROOPYA_CUSTOMER_STATUS')
    print("Final_status_df",final_status_df.columns)

    df_1 = merged3_df.drop(columns = ['ACCOUNT_TYPE', 'CONTRIBUTOR_TYPE', 'OWNERSHIP_IND', 'ACCOUNT_STATUS','ROOPYA_ACCOUNT_STATUS', 'Loan Category'])
    df_2 = final_status_df.drop(columns = ['CREDIT_REPORT_ID'])
    

    df1 = df_1.reset_index(drop=True)
    df2 = df_2.reset_index(drop=True)

    result_df = pd.concat([df1, df2], axis = 1)

    final_df = result_df.rename(columns={'Home Loan': 'ACCOUNT_TYPE_Home Loan'})

    #final_df.isnull().sum()

    #final_df['ROOPYA_CUSTOMER_STATUS'].value_counts()

    #print("No of customers with Auto Loan: ", len(final_df[final_df["ACCOUNT_TYPE_Auto Loan"] != 0]))
    #print("No of customers with Credit Card: ", len(final_df[final_df["ACCOUNT_TYPE_Credit Card"] != 0]))
    #print("No of customers with Home Loan: ", len(final_df[final_df["ACCOUNT_TYPE_Home Loan"] != 0]))
    #print("No of customers with Personal Loan: ", len(final_df[final_df["ACCOUNT_TYPE_Personal Loan"] != 0]))

    #pd.set_option('display.float_format', '{:.2f}'.format)
    #final_df.describe()

    df10 = final_df[(final_df['Active_OVERDUE_AMOUNT'] >= 0)]



    df10 = df10.drop(columns = ['Closed_CURRENT_BALANCE', 'Closed_OVERDUE_AMOUNT'], axis = 1)
    df10 = df10[df10['CREDIT_REPORT_ID']!= 'RoopyaDummy']
    print(df10)
    csv_data = df10.to_csv(index=False)
    bucket = storage_client.get_bucket(bucket_name)

    # Create a GCS Blob and upload the CSV data
    blob = bucket.blob(object_path)
    blob.upload_from_string(csv_data, content_type='text/csv')

    print(f'DataFrame saved to GCS: gs://{bucket_name}/{object_path}')
        
    return    



@functions_framework.http
def save_to_bigquery(request):
    request_json = request.get_json(silent=True)

    if request_json and isinstance(request_json, list) and len(request_json) == 1:
        data = request_json[0]
        products = data.get('products', [])
        request_json = data.get('request_json', [])
        # Generate a random token for the new table
        #token = random.choices(string.ascii_letters, k=12)
        #token = token_gen()
        table_id = 'PROD_' + ''.join(token)

        # Create a reference to the BigQuery table with the random token
        table_ref = client.dataset(DATASET_ID).table(table_id)

        # Create the table with the defined schema if it doesn't exist
        try:
            table = bigquery.Table(table_ref, schema=SCHEMA)
            table = client.create_table(table)
        except Exception as e:
            pass

        # Prepare the rows to insert into the table for products
        rows_to_insert_products = []
        for item in products:
            if 'Model ID' in item and 'Name' in item and 'Product' in item and 'Purpose' in item and 'Status' in item and 'Description' in item and 'Created Date' in item and 'Modified Date' in item and 'Model Test Report':
                Model_ID = item['Model ID']
                Name = item['Name']
                Product = item['Product']
                Purpose = item['Purpose']
                Status = item['Status']
                Description = item['Description']
                Created_Date  = item['Created Date']
                Modified_Date = item['Modified Date']
                Model_Test_Report = item['Model Test Report']
                row = {
                    'Model ID': Model_ID,
                    'Name': Name,
                    'Product': Product,
                    'Purpose': Purpose,
                    'Status': Status,
                    'Description': Description,
                    'Created Date': Created_Date,
                    'Modified Date': Modified_Date,
                    'Status': Status,
                    'Model Test Report': Model_Test_Report
                }
                rows_to_insert_products.append(row)
        if rows_to_insert_products:     
            # Save the product data to GCS
            df = pd.DataFrame(rows_to_insert_products)
            csv_data = df.to_csv(index=False)
            object_path_product = f'Swarnavo/Pipeline/{base_filename_product}_{token}.csv'
            bucket = storage_client.get_bucket(bucket_name)
            blob = bucket.blob(object_path_product)
            blob.upload_from_string(csv_data, content_type='text/csv')
            print(f'DataFrame saved to GCS: gs://{bucket_name}/{object_path_product}')               

        # Prepare the rows to insert into the table
        rows_to_insert = []
        for item in request_json:
            if 'CREDT_RPT_ID' in item and 'STATUS' in item and 'ERROR' in item and 'PERFORM_CNS_SCORE' in item and 'PERFORM_CNS_SCORE_DESCRIPTION' in item and 'INCOME_BAND' in item and 'INCOME_BAND_DESCRIPTION' in item and 'PRIMARY_NO_OF_ACCOUNTS' in item and 'PRIMARY_ACTIVE_ACCOUNTS' in item and 'PRIMARY_OVERDUE_ACCOUNTS' in item and 'PRIMARY_CURRENT_BALANCE' in item and 'PRIMARY_SANCTIONED_AMOUNT' in item and 'PRIMARY_DISTRIBUTED_AMOUNT' in item and 'SECONDARY_NO_OF_ACCOUNTS' in item and 'SECONDARY_ACTIVE_ACCOUNTS' in item and 'SECONDARY_OVERDUE_ACCOUNTS' in item and 'SECONDARY_CURRENT_BALANCE' in item and 'SECONDARY_SANCTIONED_AMOUNT' in item and 'SECONDARY_DISBURSED_AMOUNT' in item and 'PRIMARY_INSTALLMENT_AMOUNT' in item and 'SECONDARY_INSTALLMENT_AMOUNT' in item and 'NEW_ACCOUNTS_IN_LAST_SIX_MONTHS' in item and 'DELINQUENT_ACCOUNTS_IN_LAST_SIX_MONTHS' in item and 'AVERAGE_ACCOUNT_AGE' in item and 'CREDIT_HISTORY_LENGTH' in item and 'NO_OF_INQUIRIES' in item and 'BRANCH' in item and 'KENDRA' in item and 'SELF_INDICATOR' in item and 'MATCH_TYPE' in item and 'ACCOUNT_NUMBER' in item and 'ACCOUNT_TYPE' in item and 'CONTRIBUTOR_TYPE' in item and 'DATE_REPORTED' in item and 'OWNERSHIP_IND' in item and 'ACCOUNT_STATUS' in item and 'DISBURSED_DATE' in item and 'CLOSE_DATE' in item and 'LAST_PAYMENT_DATE' in item and 'CREDIT_LIMIT_SANCTION_AMOUNT' in item and 'DISBURSED_AMOUNT_HIGH_CREDIT' in item and 'INSTALLMENT_AMOUNT' in item and 'CURRENT_BALANCE' in item and 'INSTALLMENT_FREQUENCY' in item and 'WRITE_OFF_DATE' in item and 'OVERDUE_AMOUNT' in item and 'WRITE_OFF_AMOUNT' in item and 'ASSET_CLASS' in item and 'ACCOUNT_REMARKS' in item and 'LINKED_ACCOUNTS' in item and 'REPORTED_DATE_HISTORY' in item and 'DAYS_PAST_DUE_HISTORY' in item and 'ASSET_CLASS_HISTORY' in item and 'HIGH_CREDIT_HISTORY' in item and 'CURRENT_BALANCE_HISTORY' in item and 'DERIVED_ACCOUNT_STATUS_HISTORY' in item and 'AMOUNT_OVERDUE_HISTORY' in item and 'AMOUNT_PAID_HISTORY' in item and 'INCOME' in item and 'INCOME_INDICATOR' in item and 'TENURE' in item and 'OCCUPATION' in item and 'CREDIT_GRANTOR' in item and 'INQUIRY_DATE' in item and 'OWNERSHIP_TYPE' in item and 'PURPOSE' in item and 'AMOUNT' in item and 'REMARKS' in item and 'BRANCH_ID' in item and 'DATE_OF_BIRTH' in item and 'GENDER' in item and 'STATE_1' in item and 'STATE_2' in item and 'STATE_3' in item:
                CREDT_RPT_ID = item['CREDT_RPT_ID']
                STATUS = item['STATUS']
                ERROR = item['ERROR']
                PERFORM_CNS_SCORE = item['PERFORM_CNS_SCORE']
                PERFORM_CNS_SCORE_DESCRIPTION = item['PERFORM_CNS_SCORE_DESCRIPTION']
                INCOME_BAND = item['INCOME_BAND']
                INCOME_BAND_DESCRIPTION = item['INCOME_BAND_DESCRIPTION']
                PRIMARY_NO_OF_ACCOUNTS = item['PRIMARY_NO_OF_ACCOUNTS']
                PRIMARY_ACTIVE_ACCOUNTS = item['PRIMARY_ACTIVE_ACCOUNTS']
                PRIMARY_OVERDUE_ACCOUNTS = item['PRIMARY_OVERDUE_ACCOUNTS']
                PRIMARY_CURRENT_BALANCE = item['PRIMARY_CURRENT_BALANCE']
                PRIMARY_SANCTIONED_AMOUNT = item['PRIMARY_SANCTIONED_AMOUNT']
                PRIMARY_DISTRIBUTED_AMOUNT = item['PRIMARY_DISTRIBUTED_AMOUNT']
                SECONDARY_NO_OF_ACCOUNTS = item['SECONDARY_NO_OF_ACCOUNTS']
                SECONDARY_ACTIVE_ACCOUNTS = item['SECONDARY_ACTIVE_ACCOUNTS']
                SECONDARY_OVERDUE_ACCOUNTS = item['SECONDARY_OVERDUE_ACCOUNTS']
                SECONDARY_CURRENT_BALANCE = item['SECONDARY_CURRENT_BALANCE']
                SECONDARY_SANCTIONED_AMOUNT = item['SECONDARY_SANCTIONED_AMOUNT']
                SECONDARY_DISBURSED_AMOUNT = item['SECONDARY_DISBURSED_AMOUNT']
                PRIMARY_INSTALLMENT_AMOUNT = item['PRIMARY_INSTALLMENT_AMOUNT']
                SECONDARY_INSTALLMENT_AMOUNT = item['SECONDARY_INSTALLMENT_AMOUNT']
                NEW_ACCOUNTS_IN_LAST_SIX_MONTHS = item['NEW_ACCOUNTS_IN_LAST_SIX_MONTHS']
                DELINQUENT_ACCOUNTS_IN_LAST_SIX_MONTHS = item['DELINQUENT_ACCOUNTS_IN_LAST_SIX_MONTHS']
                AVERAGE_ACCOUNT_AGE = item['AVERAGE_ACCOUNT_AGE']
                CREDIT_HISTORY_LENGTH = item['CREDIT_HISTORY_LENGTH']
                NO_OF_INQUIRIES = item['NO_OF_INQUIRIES']
                BRANCH = item['BRANCH']
                KENDRA = item['KENDRA']
                SELF_INDICATOR = item['SELF_INDICATOR']
                MATCH_TYPE = item['MATCH_TYPE']
                ACCOUNT_NUMBER = item['ACCOUNT_NUMBER']
                ACCOUNT_TYPE = item['ACCOUNT_TYPE']
                CONTRIBUTOR_TYPE = item['CONTRIBUTOR_TYPE']
                DATE_REPORTED = item['DATE_REPORTED']
                OWNERSHIP_IND = item['OWNERSHIP_IND']
                ACCOUNT_STATUS = item['ACCOUNT_STATUS']
                DISBURSED_DATE = item['DISBURSED_DATE']
                CLOSE_DATE = item['CLOSE_DATE']
                LAST_PAYMENT_DATE = item['LAST_PAYMENT_DATE']
                CREDIT_LIMIT_SANCTION_AMOUNT = item['CREDIT_LIMIT_SANCTION_AMOUNT']
                DISBURSED_AMOUNT_HIGH_CREDIT = item['DISBURSED_AMOUNT_HIGH_CREDIT']
                INSTALLMENT_AMOUNT = item['INSTALLMENT_AMOUNT']
                CURRENT_BALANCE = item['CURRENT_BALANCE']
                INSTALLMENT_FREQUENCY = item['INSTALLMENT_FREQUENCY']
                WRITE_OFF_DATE = item['WRITE_OFF_DATE']
                OVERDUE_AMOUNT = item['OVERDUE_AMOUNT']
                WRITE_OFF_AMOUNT = item['WRITE_OFF_AMOUNT']
                ASSET_CLASS = item['ASSET_CLASS']
                ACCOUNT_REMARKS = item['ACCOUNT_REMARKS']
                LINKED_ACCOUNTS = item['LINKED_ACCOUNTS']
                REPORTED_DATE_HISTORY = item['REPORTED_DATE_HISTORY']
                DAYS_PAST_DUE_HISTORY = item['DAYS_PAST_DUE_HISTORY']
                ASSET_CLASS_HISTORY = item['ASSET_CLASS_HISTORY']
                HIGH_CREDIT_HISTORY = item['HIGH_CREDIT_HISTORY']
                CURRENT_BALANCE_HISTORY = item['CURRENT_BALANCE_HISTORY']
                DERIVED_ACCOUNT_STATUS_HISTORY = item['DERIVED_ACCOUNT_STATUS_HISTORY']
                AMOUNT_OVERDUE_HISTORY = item['AMOUNT_OVERDUE_HISTORY']
                AMOUNT_PAID_HISTORY = item['AMOUNT_PAID_HISTORY']
                INCOME = item['INCOME']
                INCOME_INDICATOR = item['INCOME_INDICATOR']
                TENURE = item['TENURE']
                OCCUPATION = item['OCCUPATION']
                CREDIT_GRANTOR = item['CREDIT_GRANTOR']
                INQUIRY_DATE = item['INQUIRY_DATE']
                OWNERSHIP_TYPE = item['OWNERSHIP_TYPE']
                PURPOSE = item['PURPOSE']
                AMOUNT = item['AMOUNT']
                REMARKS = item['REMARKS']
                BRANCH_ID = item['BRANCH_ID']
                DATE_OF_BIRTH = item['DATE_OF_BIRTH']
                GENDER = item['GENDER']
                STATE_1 = item['STATE_1']
                STATE_2 = item['STATE_2']
                STATE_3 = item['STATE_3']
                
                
                row = {
                        'CREDT_RPT_ID': CREDT_RPT_ID,
                        'STATUS': STATUS,
                        'ERROR': ERROR,
                        'PERFORM_CNS_SCORE': PERFORM_CNS_SCORE,
                        'PERFORM_CNS_SCORE_DESCRIPTION': PERFORM_CNS_SCORE_DESCRIPTION,
                        'INCOME_BAND': INCOME_BAND,
                        'INCOME_BAND_DESCRIPTION': INCOME_BAND_DESCRIPTION,
                        'PRIMARY_NO_OF_ACCOUNTS': PRIMARY_NO_OF_ACCOUNTS,
                        'PRIMARY_ACTIVE_ACCOUNTS': PRIMARY_ACTIVE_ACCOUNTS,
                        'PRIMARY_OVERDUE_ACCOUNTS': PRIMARY_OVERDUE_ACCOUNTS,
                        'PRIMARY_CURRENT_BALANCE': PRIMARY_CURRENT_BALANCE,
                        'PRIMARY_SANCTIONED_AMOUNT': PRIMARY_SANCTIONED_AMOUNT,
                        'PRIMARY_DISTRIBUTED_AMOUNT': PRIMARY_DISTRIBUTED_AMOUNT,
                        'SECONDARY_NO_OF_ACCOUNTS': SECONDARY_NO_OF_ACCOUNTS,
                        'SECONDARY_ACTIVE_ACCOUNTS': SECONDARY_ACTIVE_ACCOUNTS,
                        'SECONDARY_OVERDUE_ACCOUNTS': SECONDARY_OVERDUE_ACCOUNTS,
                        'SECONDARY_CURRENT_BALANCE': SECONDARY_CURRENT_BALANCE,
                        'SECONDARY_SANCTIONED_AMOUNT': SECONDARY_SANCTIONED_AMOUNT,
                        'SECONDARY_DISBURSED_AMOUNT': SECONDARY_DISBURSED_AMOUNT,
                        'PRIMARY_INSTALLMENT_AMOUNT': PRIMARY_INSTALLMENT_AMOUNT,
                        'SECONDARY_INSTALLMENT_AMOUNT': SECONDARY_INSTALLMENT_AMOUNT,
                        'NEW_ACCOUNTS_IN_LAST_SIX_MONTHS': NEW_ACCOUNTS_IN_LAST_SIX_MONTHS,
                        'DELINQUENT_ACCOUNTS_IN_LAST_SIX_MONTHS': DELINQUENT_ACCOUNTS_IN_LAST_SIX_MONTHS,
                        'AVERAGE_ACCOUNT_AGE': AVERAGE_ACCOUNT_AGE,
                        'CREDIT_HISTORY_LENGTH': CREDIT_HISTORY_LENGTH,
                        'NO_OF_INQUIRIES': NO_OF_INQUIRIES,
                        'BRANCH': BRANCH,
                        'KENDRA': KENDRA,
                        'SELF_INDICATOR': SELF_INDICATOR,
                        'MATCH_TYPE': MATCH_TYPE,
                        'ACCOUNT_NUMBER': ACCOUNT_NUMBER,
                        'ACCOUNT_TYPE': ACCOUNT_TYPE,
                        'CONTRIBUTOR_TYPE': CONTRIBUTOR_TYPE,
                        'DATE_REPORTED': DATE_REPORTED,
                        'OWNERSHIP_IND': OWNERSHIP_IND,
                        'ACCOUNT_STATUS': ACCOUNT_STATUS,
                        'DISBURSED_DATE': DISBURSED_DATE,
                        'CLOSE_DATE': CLOSE_DATE,
                        'LAST_PAYMENT_DATE': LAST_PAYMENT_DATE,
                        'CREDIT_LIMIT_SANCTION_AMOUNT': CREDIT_LIMIT_SANCTION_AMOUNT,
                        'DISBURSED_AMOUNT_HIGH_CREDIT': DISBURSED_AMOUNT_HIGH_CREDIT,
                        'INSTALLMENT_AMOUNT': INSTALLMENT_AMOUNT,
                        'CURRENT_BALANCE': CURRENT_BALANCE,
                        'INSTALLMENT_FREQUENCY': INSTALLMENT_FREQUENCY,
                        'WRITE_OFF_DATE': WRITE_OFF_DATE,
                        'OVERDUE_AMOUNT': OVERDUE_AMOUNT,
                        'WRITE_OFF_AMOUNT': WRITE_OFF_AMOUNT,
                        'ASSET_CLASS': ASSET_CLASS,
                        'ACCOUNT_REMARKS': ACCOUNT_REMARKS,
                        'LINKED_ACCOUNTS': LINKED_ACCOUNTS,
                        'REPORTED_DATE_HISTORY': REPORTED_DATE_HISTORY,
                        'DAYS_PAST_DUE_HISTORY': DAYS_PAST_DUE_HISTORY,
                        'ASSET_CLASS_HISTORY': ASSET_CLASS_HISTORY,
                        'HIGH_CREDIT_HISTORY': HIGH_CREDIT_HISTORY,
                        'CURRENT_BALANCE_HISTORY': CURRENT_BALANCE_HISTORY,
                        'DERIVED_ACCOUNT_STATUS_HISTORY': DERIVED_ACCOUNT_STATUS_HISTORY,
                        'AMOUNT_OVERDUE_HISTORY': AMOUNT_OVERDUE_HISTORY,
                        'AMOUNT_PAID_HISTORY': AMOUNT_PAID_HISTORY,
                        'INCOME': INCOME,
                        'INCOME_INDICATOR': INCOME_INDICATOR,
                        'TENURE': TENURE,
                        'OCCUPATION': OCCUPATION,
                        'CREDIT_GRANTOR': CREDIT_GRANTOR,
                        'INQUIRY_DATE': INQUIRY_DATE,
                        'OWNERSHIP_TYPE': OWNERSHIP_TYPE,
                        'PURPOSE': PURPOSE,
                        'AMOUNT': AMOUNT,
                        'REMARKS': REMARKS,
                        'BRANCH_ID': BRANCH_ID,
                        'DATE_OF_BIRTH': DATE_OF_BIRTH,
                        'GENDER': GENDER,
                        'STATE_1': STATE_1,
                        'STATE_2': STATE_2,
                        'STATE_3': STATE_3
                }
                rows_to_insert.append(row)

        # Insert the rows into the BigQuery table with the defined schema
        if rows_to_insert:
            result = insert_rows_into_bigquery(table_ref, rows_to_insert)
            print(result)
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
            
            #return f"All BigQuery steps are successful, Table deleated as: '{preprocess_table_id_2}'."
            return f"All BigQuery steps are successful, Table deleated as: '{preprocess_table_id_2}' and Preprocessed python DataFrame saved to GCS: gs://{bucket_name}/{object_path}."

        else:
            return "No valid data to insert."
    else:
        return "Invalid input format or no data to process."
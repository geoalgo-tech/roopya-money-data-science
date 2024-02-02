##Import the packages
from google.cloud import bigquery
from google.cloud import storage
from pandas_gbq import to_gbq
from datetime import datetime
import pandas as pd
import numpy as np
import pickle
import json
import os
import seaborn as sns
import matplotlib.pyplot as plt
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from statsmodels.stats.outliers_influence import variance_inflation_factor
import statsmodels.formula.api as sm
import statsmodels.api as sma
import seaborn as sns
from patsy import dmatrices
from sklearn.metrics import roc_curve
from sklearn.metrics import confusion_matrix
from statsmodels.stats.outliers_influence import variance_inflation_factor
from sklearn.metrics import roc_curve, auc

print('Changes')
confs = {}
Home_Loan = ['Housing Loan', 'Property Loan', 'Leasing', 'Microfinance Housing Loan']
Credit_Loan = ['Credit Card', 'Corporate Credit Card', 'Kisan Credit Card', 'Secured Credit Card', 'Loan on Credit Card']
Auto_Loan = ['Auto Loan (Personal)','Two-Wheeler Loan','Commercial Vehicle Loan','Used Car Loan','Used Tractor Loan','Tractor Loan']
Personal_Loan = ['Personal Loan',  'Consumer Loan','Microfinance Personal Loan', 'Loan to Professional']
Overdraft=['Overdraft','Loan Aganist Bank Deposits','OD on Savings Account']

def logger(message):
    timestamp=datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    f=open("logs.txt",'a')
    message=timestamp+"|  "+str(message)+"\n"
    print(message)
    f.write(message)
    f.close()

def get_confs():
    global confs
    conf_df = pd.read_csv("confs.txt", delimiter='=', names=['KEY', 'VALUE'])
    confs = dict(zip(conf_df['KEY'], conf_df['VALUE']))
    return confs

def Loan_categories(confs):
    sub_types = ''
    if confs['PRODUCT_TYPE'] == 'Home_Loan':
        for element in Home_Loan:
            sub_types = sub_types + "'" + element + "'" + ','
        sub_types = sub_types[:len(sub_types) - 1]
    elif confs['PRODUCT_TYPE'] == 'Auto_Loan':
        for element in Auto_Loan:
            sub_types = sub_types + "'" + element + "'" + ','
        sub_types = sub_types[:len(sub_types) - 1]
    elif confs['PRODUCT_TYPE'] == 'Personal_Loan':
        for element in Personal_Loan:
            sub_types = sub_types + "'" + element + "'" + ','
        sub_types = sub_types[:len(sub_types) - 1]
    elif confs['PRODUCT_TYPE'] == 'Credit_Card':
        for element in Credit_Loan:
            sub_types = sub_types + "'" + element + "'" + ','
        sub_types = sub_types[:len(sub_types) - 1]
    elif confs['PRODUCT_TYPE'] == 'Overdraft':
        for element in Overdraft:
            sub_types = sub_types + "'" + element + "'" + ','
        sub_types = sub_types[:len(sub_types) - 1]
    else:
        logger('Not a valid information')
    
    return sub_types

get_confs()
logger(confs['PRODUCT_TYPE'])

client = bigquery.Client()
project_id = f"{confs['PROJECT_ID']}"
destination_table_id = f"{confs['DESTINATION_ID']}"


def remove_contributor_types(confs):
    loan_subtypes = confs['OWNERSHIP_IND_REMOVE'].split(',')
    loan_subtypes_list = []
    
    for element in loan_subtypes:
        loan_subtypes_list.append(element.strip())
        
    #print(loan_subtypes)
    loan_exclude=''
    for element in loan_subtypes_list:
        loan_exclude = loan_exclude + "'" + element + "'" + ','
    loan_exclude = loan_exclude[:len(loan_exclude) - 1]    

    return loan_exclude

#sub_types = Loan_categories(confs)
def basic_cleaning_steps():
    """
    Here the basic data cleaning is going on
    """
    global confs
    
    #df1 = pd.read_excel(f"{confs['RAW_f_DATA']}")
    ##print("Reading Excel File")
    #file_path_w = '{}'.format(confs['APPLICATION_DATA'])
    #df1.to_csv(file_path_w, index = False, index_label = False)

    df2 = pd.read_csv(f"{confs['APPLICATION_DATA']}")

    logger('Read successful!!!!!!')
    df = df2[['CREATED DATE','CUSTOMER ID','PHONE','GENDER','PAN','MARITAL STATUS','INCOME','CITY','STATE',
                 'PROFESSION TYPE','RESIDENCE TYPE','TOTAL WORK EXPERIENCE','QUALIFICATION','EXISTING EMI', 
                 'COMPANY TYPE','EMI AMOUNT','DESIRED LOAN AMOUNT','TENURE','LOAN PURPOSE']]

    def snake_case_and_lowercase(column_name):
        return column_name.replace(' ', '_').lower()

    df.columns = df.columns.map(snake_case_and_lowercase)

    df['gender'] = df['gender'].replace([None, 'Third gender'], 'Others')

    df['marital_status'] = df['marital_status'].replace({'married': 'Married', 'single': 'Single', 'unmarried': 'Single'})

    def qstn_replace(data, column_name):
        pattern1 = r'\?+|\d+'
        data[column_name] = data[column_name].str.replace(pattern1, 'Others')
        pattern2 = r'\b(Others\s*)+\b'
        data[column_name] = data[column_name].str.replace(pattern2, 'Others')
        data[column_name] = data[column_name].replace([None], 'Others')
        return data

    df = qstn_replace(df, 'marital_status')
    

    df['marital_status'] = df['marital_status'].replace(['MariÃ©'], 'Others')
    #print(df.columns)
    
    def pan_valid(data,column):
        import re
        regex = "[A-Z]{5}[0-9]{4}[A-Z]{1}"
        data['is_valid_pan'] = data[column].astype(str).apply(lambda x: bool(re.match(regex, x)))
        data = data.drop(columns = [column], axis = 1)
        return data
    df = pan_valid(df, 'pan')
    df['profession_type'] = df['profession_type'].replace({'SelfEmployed': 'Self Employed/ Own Business'})
    df['profession_type'] = df['profession_type'].replace([None], 'Others')

    df['company_type'] = df['company_type'].replace({'PRIVATE_LIMITED': 'Private_Limited', 
                                                     'Private Company': 'Private_Limited', 
                                                     'Government': 'Government / PSU', 
                                                     'Sole_Proprietorship': 'Proprietorship', 
                                                     'Partnership / Proprietor': 'Proprietorship', 
                                                     'Partnership Company / Proprietor': 'Proprietorship', 
                                                     'Partnership_Firm': 'Proprietorship', 
                                                     'Partnership': 'Proprietorship', 
                                                     'Public_Sector': 'Public_Limited',
                                                     'Private Ltd.': 'Private_Limited',
                                                     'Public Ltd.': 'Public_Limited'})
    
    df['company_type'] = df['company_type'].fillna(value='Others')
    df['company_type'] = df['company_type'].replace(['Graduate', 'greater than 10 Years', '1 Year - 3 Years'], 'Others')

    df = qstn_replace(df, 'loan_purpose')

    df['loan_purpose'] = df['loan_purpose'].replace(['OTHERS', 'Others / Others', 'Others / Others-Others'], 'Others')

    df['qualification'] = df['qualification'].replace({'under graduate': 'Under Graduate', 
                                                       'UnderGraduate': 'Under Graduate',
                                                       'post graduate': 'Post Graduate',
                                                       'PostGraduate': 'Post Graduate',
                                                       'graduate': 'Graduate', 'Other': 'Others'})

    df = qstn_replace(df, 'qualification')

    df['qualification'] = df['qualification'].replace(['Others-Others-Others:Others:Others', 
                                                       'OthersYear - OthersYears'], 'Others')

    def categorize_city(city):
        if city in ['Mumbai', 'New Delhi', 'Bangalore', 'Kolkata', 'Hyderabad', 'Chennai']:
            return 'Tier_1'
        elif pd.isna(city):
            return 'Others'
        else:
            return 'Tier_2'

    df['city_tier'] = df['city'].apply(categorize_city)
    
    state_zone_mapping = {
    'Jammu and Kashmir': 'North','Punjab': 'North','Haryana': 'North','Uttar Pradesh': 'North','UP UK': 'North',
    'Uttarakhand': 'North','UTTRAKHAND': 'North','Himachal Pradesh': 'North','Uttaranchal': 'North',
    'Delhi': 'North','Rajasthan': 'North','Goa': 'West','Gujarat': 'West','Dadra and Nagar Haveli': 'West',
    'Daman and Diu': 'West','Maharashtra': 'West','Madhya Pradesh': 'Central','Chhattisgarh': 'Central',
    'Bihar': 'East','Odisha': 'East','Jharkhand': 'East','West Bengal': 'East','Odisha': 'East',
    'Tamil Nadu': 'South','Kerala': 'South','Karnataka': 'South','Andhra Pradesh': 'South','Telangana': 'South', 
    'Andhra Pradesh': 'South', 'Pondicherry': 'South', 'Not Found': 'Others'
    }
    
    df['state'] = df['state'].map(state_zone_mapping)
    df['state'] = df['state'].fillna(value='Others')

    specified_values = [ '5 Years - 10 Years','3 Years - 5 Years','greater than 10 Years','1 Year - 3 Years',
        'less than 1 Year']

    # Replace values not in the specified list with 'Others'
    df['total_work_experience'] = df['total_work_experience'].apply(lambda x: x if x in specified_values else 'Others')

    df['existing_emi'] = df['existing_emi'].replace({'yes': 'Yes', 'no': 'No', 'Graduate': 'No', 
                                                     'Private Company': 'No'})

    df['existing_emi'] = df['existing_emi'].replace([None], 'Others')

    specified_values = ['Owned by Parents/Sibling','Rented with Family','Owned by Self-Spouse',
                        'Rented with Friends','Rented- Staying Alone']

    df['residence_type'] = df['residence_type'].apply(lambda x: x if x in specified_values else 'Others')

    def convert_numeric(value):
        import re
        numeric_part = re.sub('[^0-9]', '', str(value))
        return int(numeric_part) if numeric_part else None

    df['emi_amount'] = df['emi_amount'].apply(convert_numeric)

    df['desired_loan_amount'] = pd.to_numeric(df['desired_loan_amount'], errors='coerce').fillna(0)
    df['income'] = pd.to_numeric(df['income'], errors='coerce').fillna(0)
    df['emi_amount'] = pd.to_numeric(df['emi_amount'], errors='coerce').fillna(0)
    df['tenure'] = pd.to_numeric(df['tenure'], errors='coerce').fillna(0)

    df1 = df.drop(columns = ['city'], axis = 1)
    
#     #print(df1.columns)
    df1.to_csv('Untitled Folder/Raw_Cleaned_dataset.csv', index=False,index_label=False)
    df2 = pd.read_csv('Untitled Folder/Raw_Cleaned_dataset.csv')
    
#     logger('Almost there, hold on')
#     #print(df2.head(5))
    print(df2.dtypes)
    client = bigquery.Client()
    project_id = f"{confs['PROJECT_ID']}"
    destination_table_id = f"{confs['DESTINATION_ID']}"

    df2.to_gbq(destination_table=destination_table_id, project_id=project_id, if_exists='replace')
    return

logger('Starting cleaning steps')
basic_cleaning_steps()

# print(ag.head())
# print(ag.dtypes)
# print(ag['state'].value_counts())
# error
logger('Successfully completed cleaning steps')

def model_base_func():
    """
        Calling query for defining model base data and saving it in a dataframe
        Arguments: NA
        Return: 1 dataframe
    """
    global confs
    #get_confs()  # Call get_confs to update the global confs dictionary
    client = bigquery.Client()
    sub_types = ''
    sub_types = Loan_categories(confs)
    #print(sub_types)
    loan_subtypes = ''
    loan_subtypes = remove_contributor_types(confs)
    #print(loan_subtypes)

    query = f"""WITH CTE1 AS (
                  SELECT inq.CREDT_RPT_ID, inq.PHONE_1, acc.*, app.created_date,app.customer_id,app.phone,app.gender AS gender,
                  app.is_valid_pan,app.marital_status,app.income,app.city_tier,app.state,app.profession_type,app.residence_type,
                  app.total_work_experience,app.qualification,app.existing_emi,app.company_type,app.emi_amount,
                  app.desired_loan_amount,app.tenure,app.loan_purpose,
                    ROW_NUMBER() OVER (PARTITION BY inq.CREDT_RPT_ID ORDER BY acc.DISBURSED_DT DESC) AS rn
                  FROM `{confs['INQUIRY_TABLE']}` AS inq
                  LEFT JOIN `{confs['ACCOUNT_TABLE']}` AS acc ON inq.CREDT_RPT_ID = acc.CREDT_RPT_ID
                  LEFT JOIN `{confs['PROJECT_ID']}.{confs['DESTINATION_ID']}` AS app ON inq.PHONE_1 = app.PHONE
                  WHERE 
                    ACCT_TYPE IN ({sub_types})
                    AND DATE(app.created_date) <= acc.DISBURSED_DT
                    AND acc.DISBURSED_DT <= DATE '{confs['DISBURSED_DATE_END']}'
                    AND DATE_DIFF(acc.DATE_REPORTED, acc.DISBURSED_DT, MONTH) BETWEEN {confs['DATE_DIFF_START']} AND {confs['DATE_DIFF_END']}
                    AND acc.OWNERSHIP_IND NOT IN ({loan_subtypes})
                )

                SELECT *
                FROM CTE1 
                WHERE rn = 1;"""

    #print(query)
    # Run the query
    query_job = client.query(query)
    results = query_job.result()
    df = results.to_dataframe()
    logger(df.shape)

    return df



model_base = model_base_func()
logger('Initiating Model Base function')
model_base.to_csv('Untitled Folder/Model_Base_{}.csv'.format(confs['PRODUCT_TYPE']), index=False)
#print('After model_base:', model_base.shape)
logger('Initiation done')


def historical_data_func():
    """
        Calling query for defining Historical data and saving it in a dataframe
        Arguments: NA
        Return: 1 dataframe
    """
    global confs
    client = bigquery.Client()
    sub_types = ''
    sub_types = Loan_categories(confs)
    #print(sub_types)
    loan_subtypes = ''
    loan_subtypes = remove_contributor_types(confs)
                                      
                                      
    query = f"""WITH CTE1 AS (
                SELECT inq.PHONE_1, acc.*, app.created_date, app.customer_id, app.phone, app.gender AS gender,
                    app.is_valid_pan, app.marital_status, app.income, app.city_tier, app.state, app.profession_type, app.residence_type,
                    app.total_work_experience, app.qualification, app.existing_emi, app.company_type, app.emi_amount,
                    app.desired_loan_amount, app.tenure, app.loan_purpose,
                    ROW_NUMBER() OVER (PARTITION BY inq.CREDT_RPT_ID ORDER BY acc.DISBURSED_DT DESC) AS rn
                FROM `{confs['INQUIRY_TABLE']}` AS inq
                LEFT JOIN `{confs['ACCOUNT_TABLE']}` AS acc ON inq.CREDT_RPT_ID = acc.CREDT_RPT_ID
                LEFT JOIN `{confs['PROJECT_ID']}.{confs['DESTINATION_ID']}` AS app ON inq.PHONE_1 = app.PHONE
                WHERE 
                    ACCT_TYPE IN ({sub_types})
                    AND DATE(app.created_date) <= acc.DISBURSED_DT
                    AND acc.DISBURSED_DT <= DATE '{confs['DISBURSED_DATE_END']}'
                    AND DATE_DIFF(acc.DATE_REPORTED, acc.DISBURSED_DT, MONTH) BETWEEN {confs['DATE_DIFF_START']} AND {confs['DATE_DIFF_END']}
                    AND acc.OWNERSHIP_IND NOT IN ({loan_subtypes})
            ),

            CTE2 AS (
                SELECT * FROM CTE1 
                WHERE CTE1.rn = 1
            ),

            CTE3 AS (
                SELECT 
                    CTE2.CREDT_RPT_ID, T1.*,
                    CTE2.DISBURSED_DT AS Ref_DISBURSED_DT,
                    DATE_DIFF(CTE2.DISBURSED_DT, T1.DISBURSED_DT, MONTH) AS Prev_loan_LOR,
                    LENGTH(T1.DPD___HIST) / 3 AS Count_DPD_stream,
                    DATE_ADD(T1.DATE_REPORTED, INTERVAL CAST((LENGTH(T1.DPD___HIST) / 3 * (-1) + 1) AS INT64) MONTH) AS DPD_First_month
                FROM 
                    `{confs['ACCOUNT_TABLE']}` AS T1 
                JOIN 
                    CTE2 ON CTE2.CREDT_RPT_ID = T1.CREDT_RPT_ID
                    AND CTE2.DISBURSED_DT > T1.DISBURSED_DT
            )

            SELECT *, DATE_DIFF(Ref_DISBURSED_DT, DPD_First_month, MONTH) AS DPD_month_tb_considered 
            FROM CTE3
            WHERE ACCT_TYPE NOT LIKE 'O%t';
            """

    print(query)
    # Run the query
    query_job = client.query(query)
    results = query_job.result()

    df = results.to_dataframe()
    
    return df


historical_data = historical_data_func()
logger('Initiating Historical function')
historical_data.to_csv('Untitled Folder/Historical_Data_{}.csv'.format(confs['PRODUCT_TYPE']), index=False)
#print('After historical_data:', historical_data.shape)
logger('Initiation done')

def read_ioi_func():
    
    global confs
    client = bigquery.Client()
    sub_types = ''
    sub_types = Loan_categories(confs)
    #print(sub_types)
    loan_subtypes = ''
    loan_subtypes = remove_contributor_types(confs)
    
    query = f"""WITH CTE1 AS (
                  SELECT inq.CREDT_RPT_ID, inq.PHONE_1, acc.DISBURSED_DT, acc.DATE_REPORTED, app.created_date,
                    ROW_NUMBER() OVER (PARTITION BY inq.CREDT_RPT_ID ORDER BY acc.DISBURSED_DT DESC) AS rn
                  FROM `{confs['INQUIRY_TABLE']}` AS inq
                  LEFT JOIN `{confs['ACCOUNT_TABLE']}` AS acc ON inq.CREDT_RPT_ID = acc.CREDT_RPT_ID
                  LEFT JOIN `{confs['PROJECT_ID']}.{confs['DESTINATION_ID']}` AS app ON inq.PHONE_1 = app.PHONE
                  WHERE 
                    ACCT_TYPE IN ({sub_types})
                    AND DATE(app.created_date) <= acc.DISBURSED_DT
                    AND acc.DISBURSED_DT <= DATE '{confs['DISBURSED_DATE_END']}'
                    AND DATE_DIFF(acc.DATE_REPORTED, acc.DISBURSED_DT, MONTH) BETWEEN {confs['DATE_DIFF_START']} AND {confs['DATE_DIFF_END']}
                    AND acc.OWNERSHIP_IND NOT IN ({loan_subtypes})
                ),
             CTE2 AS
                    (SELECT *
                FROM CTE1 
                WHERE rn = 1)

            SELECT T1.*, CTE2.DISBURSED_DT AS Ref_DISBURSED_DT
            FROM `{confs['IOI_TABLE']}` AS T1 
            JOIN CTE2 ON T1.CREDT_RPT_ID = CTE2.CREDT_RPT_ID 
            WHERE CTE2.DISBURSED_DT > T1.INQUIRY_DATE;
            """

    #print(query)
    # Run the query
    query_job = client.query(query)
    results = query_job.result()

    df = results.to_dataframe()

    # df.to_csv('Historical_Data.csv', index=False)
    return df

inquiry_data = read_ioi_func()
logger('Initiating IOI function')
inquiry_data.to_csv('Untitled Folder/IOI_Data_{}.csv'.format(confs['PRODUCT_TYPE']), index=False)
#print('After historical_data:', historical_data.shape)
logger('Initiation done')

def merged_data_func():
    """
        Reading the Model Base and Historical Data and performing categorization of contributor type, aggregation of account status, customer type, delinquency, disbursed and current balance so that later we can call this one function to get the training data.
        Arguments: NA
        Return: 1 dataframe
    """
    data1 = pd.read_csv('Untitled Folder/Model_Base_{}.csv'.format(confs['PRODUCT_TYPE']))
    data2 = pd.read_csv('Untitled Folder/Historical_Data_{}.csv'.format(confs['PRODUCT_TYPE']))
    data3 = pd.read_csv('Untitled Folder/IOI_Data_{}.csv'.format(confs['PRODUCT_TYPE']))
    
    def feautures_creation_his_data(df):
            df['ACCOUNT_STATUS'] = df['ACCOUNT_STATUS'].replace({
            'Written Off': 'Other',
            'Settled': 'Other',
            'Suit Filed': 'Other',
            'Sold/Purchased': 'Other',
            'Restructured': 'Other',
            'WILFUL DEFAULT': 'Other',
            'SUIT FILED (WILFUL DEFAULT)': 'Other',
            'Post Write Off Settled': 'Other',
            'Loan Approved - Not yet disbursed': 'Other'
        })
#             def replace_account_status(row):
#                 if row['ACCOUNT_STATUS'] == 'Active':
#                     return 'Active'
#                 elif row['ACCOUNT_STATUS'] == 'Closed':
#                     return 'Closed'
#                 elif row['ACCOUNT_STATUS'] == 'Delinquent':
#                     return 'Delinquent'
#                 else:
#                     return 'Others'
                
#             df['Replaced_Account_Status'] = df.apply(replace_account_status, axis=1)

            mapping_dict = {
            'Gold Loan': ['Personal Loan', 'Secured'],
            'Overdraft': ['Personal Loan', 'Unsecured'],
            'GECL Loan secured': ['Personal Loan', 'Secured'],
            'Construction Equipment Loan': ['Personal Loan', 'Secured'],
            'Used Car Loan': ['Auto Loan', 'Secured'],
            'Loan Against Shares / Securities': ['Personal Loan', 'Secured'],
            'Business Loan Against Bank Deposits': ['Personal Loan', 'Secured'],
            'Microfinance Business Loan': ['Personal Loan', 'Unsecured'],
            'SHG Group': ['Personal Loan', 'Unsecured'],
            'Commercial Equipment Loan': ['Personal Loan', 'Secured'],
            'Credit Card': ['Credit Card', 'Unsecured'],
            'Personal Loan': ['Personal Loan', 'Unsecured'],
            'Housing Loan': ['Home Loan', 'Secured'],
            'Loan to Professional': ['Personal Loan', 'Unsecured'],
            'Business Loan Priority Sector  Agriculture': ['Personal Loan', 'Secured'],
            'Commercial Vehicle Loan': ['Auto Loan', 'Secured'],
            'Auto Loan (Personal)': ['Auto Loan', 'Secured'],
            'Loan on Credit Card': ['Credit Card', 'Unsecured'],
            'Education Loan': ['Personal Loan', 'Unsecured'],
            'Microfinance Others': ['Personal Loan', 'Unsecured'],
            'JLG Group': ['Personal Loan', 'Unsecured'],
            'Staff Loan': ['Personal Loan', 'Unsecured'],
            'Individual': ['Personal Loan', 'Unsecured'],
            'Telco Landline': ['Personal Loan', 'Unsecured'],
            'Telco Broadband': ['Personal Loan', 'Unsecured'],
            'Telco Wireless': ['Personal Loan', 'Unsecured'],
            'Leasing': ['Home Loan', 'Secured'],
            'Business Loan Priority Sector  Small Business': ['Personal Loan', 'Secured'],
            'Business Loan Unsecured': ['Personal Loan', 'Unsecured'],
            'Business Loan General': ['Personal Loan', 'Unsecured'],
            'Kisan Credit Card': ['Credit Card', 'Secured'],
            'Loan Against Bank Deposits': ['Personal Loan', 'Secured'],
            'Business Loan Priority Sector  Others': ['Personal Loan', 'Secured'],
            'Pradhan Mantri Awas Yojana - CLSS': ['Home Loan', 'Secured'],
            'Other': ['Personal Loan', 'Unsecured'],
            'Consumer Loan': ['Personal Loan', 'Unsecured'],
            'Tractor Loan': ['Auto Loan', 'Secured'],
            'Property Loan': ['Home Loan', 'Secured'],
            'Business Loan - Secured': ['Personal Loan', 'Secured'],
            'Corporate Credit Card': ['Credit Card', 'Unsecured'],
            'JLG Individual': ['Personal Loan', 'Unsecured'],
            'Prime Minister Jaan Dhan Yojana - Overdraft': ['Personal Loan', 'Unsecured'],
            'Business Non-Funded Credit Facility General': ['Personal Loan', 'Unsecured'],
            'GECL Loan unsecured': ['Personal Loan', 'Unsecured'],
            'Non-Funded Credit Facility': ['Credit Card', 'Unsecured'],
            'Secured Credit Card': ['Credit Card', 'Secured'],
            'Two-Wheeler Loan': ['Auto Loan', 'Secured'],
            'Microfinance Personal Loan': ['Personal Loan', 'Unsecured'],
            'Mudra Loans - Shishu / Kishor / Tarun': ['Personal Loan', 'Unsecured'],
            'Microfinance Housing Loan': ['Home Loan', 'Secured'],
            'Business Non-Funded Credit Facility-Priority Sector-Agriculture': ['Personal Loan', 'Secured'],
            'SHG Individual': ['Personal Loan', 'Unsecured'],
            'Business Non-Funded Credit Facility-Priority Sector- Small Business': ['Personal Loan', 'Secured'],

        }

            df['Product Type'] = df['ACCT_TYPE'].map(lambda x: mapping_dict[x][0] if x in mapping_dict else 'Unknown')
            df['Loan Security'] = df['ACCT_TYPE'].map(lambda x: mapping_dict[x][1] if x in mapping_dict else 'Unknown')

            # Create a pivot table
            df_pivoted = df.pivot_table(
                index='CREDT_RPT_ID',
                columns=['ACCOUNT_STATUS', 'Product Type'],
                aggfunc='size',
                fill_value=0
            )

            # Rename columns for better clarity
            df_pivoted.columns = [f'No of {status} {loan_type}' for (status, loan_type) in df_pivoted.columns]
            df_pivoted.reset_index(inplace=True)

            df_pivoted_1 = df.pivot_table(
            index='CREDT_RPT_ID',
            columns=['Loan Security'],
            aggfunc='size',
            fill_value=0
        )

            df_pivoted_1.columns = [f'No of {loan_security}' for loan_security in df_pivoted_1.columns]

            # Reset index if needed
            df_pivoted_1.reset_index(inplace=True)
            df_pivoted_2 = df.pivot_table(
                index='CREDT_RPT_ID',
                columns=['Product Type'],
                values='_DISBURSED_AMT_HIGH_CREDIT',
                aggfunc='max',
                fill_value=0
            )

            df_pivoted_2.columns = [f'Max Disbursed Amount {loan_type}' for loan_type in df_pivoted_2.columns]
            df_pivoted_2.reset_index(inplace=True) 
            merged_df = df_pivoted.merge(df_pivoted_1, on='CREDT_RPT_ID', how='left')
            merged_df_1 = merged_df.merge(df_pivoted_2, on='CREDT_RPT_ID', how='left')
            df['MAX_DISBURSED_AMT_HIGH_CREDIT'] = df.groupby('CREDT_RPT_ID')['_DISBURSED_AMT_HIGH_CREDIT'].transform('max')
            df_3 = df[['CREDT_RPT_ID', 'MAX_DISBURSED_AMT_HIGH_CREDIT']].drop_duplicates()
            merged_df_2 = merged_df_1.merge(df_3, on='CREDT_RPT_ID', how='left')

            # df.to_csv('Historical_Data.csv', index=False)
            return merged_df_2
    
    #Merging with data1 at the end of all merges
    product_type_df = feautures_creation_his_data(data2)
    
    def acc_status_change(df, column):
        df['CLOSE_DT'] = pd.to_datetime(df['CLOSE_DT'], errors = 'coerce')
        df['Ref_DISBURSED_DT'] = pd.to_datetime(df['Ref_DISBURSED_DT'], errors = 'coerce')
        mask1 = df['CLOSE_DT'] > df['Ref_DISBURSED_DT']
        mask2 = df['CLOSE_DT'] > '2022-10-31' 
        df.loc[mask1, column] = 'Active'
        df.loc[mask2, column] = 'Active'
        return df

    data2 = acc_status_change(data2, 'ACCOUNT_STATUS')
    
    def pivot_and_aggregation_of_inquiry(df_ioi):
        """
        Aggregation based on Inquiry type and return aggregated inquiry with unique Credit Report Id.
        Arguments: 1 dataframe
        Return: 1 dataframe
        """
        # df_ioi_counts=pd.DataFrame(df_ioi['CREDT_RPT_ID'].value_counts())
        # df_ioi_counts.reset_index(inplace=True)
        # df_ioi_counts.rename(columns={"CREDT_RPT_ID":"Number of Inquiry","index":"CREDT_RPT_ID"},inplace=True)
        df_ioi['INQUIRY_DATE'] = pd.to_datetime(df_ioi['INQUIRY_DATE'])
        df_ioi['Ref_DISBURSED_DT'] = pd.to_datetime(df_ioi['Ref_DISBURSED_DT'])

        # Calculate the date two months before 'Ref_DISBURSED_DT'
        df_ioi['TwoMonthsBefore'] = df_ioi['Ref_DISBURSED_DT'] - pd.DateOffset(months=2)

        # Filter rows where INQUIRY_DATE is within the last two months before Ref_DISBURSED_DT
        df_filtered = df_ioi[(df_ioi['INQUIRY_DATE'] >= df_ioi['TwoMonthsBefore']) & (df_ioi['INQUIRY_DATE'] < df_ioi['Ref_DISBURSED_DT'])]

        # Group by CREDT_RPT_ID and count the occurrences
        df_ioi_counts = df_filtered.groupby('CREDT_RPT_ID').size().reset_index(name='Number of Inquiry')
        
        return df_ioi_counts
    
    #Call the pivot_and_aggregation_of_inquiry to perform action, will use this at the end.
    data3 = pivot_and_aggregation_of_inquiry(data3)
    

    def contributor_categories(df):
        """
        Categorization based on Contributor type and return contributor categories
        Arguments: 1 dataframe
        Return: 1 dataframe
        """
        categories = {
            'Category 1': ['NBF', 'CCC'],
            'Category 2': ['COP', 'OFI', 'HFC', 'RRB', 'MFI'],
            'Category 3': ['FRB', 'NAB', 'PRB', 'SFB'],
            'Category 4': ['ARC']
        }

        # Create a new column to represent the category for each entity
        df['Category'] = df['CONTRIBUTOR_TYPE'].apply(lambda x: next((key for key, values in categories.items() if x in values), None))

        # Pivot the DataFrame to create separate columns for each category
        ct_df = df.pivot_table(index='CREDT_RPT_ID', columns='Category', values='CONTRIBUTOR_TYPE', aggfunc='count', fill_value=0)

        # Reset the index to make 'Entity' a column again
        ct_df.reset_index(inplace=True)

        # Rename columns for better readability
        ct_df.rename_axis(columns=None, inplace=True)

        # Rename columns for better readability
        ct_df.rename(columns={'Category 1': 'CONTRIBUTOR_TYPE_NBF_CCC',
                              'Category 2': 'CONTRIBUTOR_TYPE_COP_OFI_HFC_RRB_MFI',
                              'Category 3': 'CONTRIBUTOR_TYPE_FRB_NAB_PRB_SFB',
                              'Category 4': 'CONTRIBUTOR_TYPE_ARC'}, inplace=True)
        return ct_df

    def account_status(df):
        """
        Aggregation based on Account Status and return countof each account status for all customers.
        Arguments: 1 dataframe
        Return: 1 dataframe
        """
        categories = {
            'Category 1': ['Active'],
            'Category 2': ['Closed'],
            'Category 3': ['Delinquent', 'Written Off', 'Settled', 'Sold/Purchased', 'Restructured', 'Suit Filed',
                           'WILFUL DEFAULT', 'SUIT FILED (WILFUL DEFAULT)']
        }

        df['Acc_St_Category'] = df['ACCOUNT_STATUS'].apply(
            lambda x: next((key for key, values in categories.items() if x in values), None))

        as_df = df.pivot_table(index='CREDT_RPT_ID', columns='Acc_St_Category', values='ACCOUNT_STATUS',
                              aggfunc='count', fill_value=0)

        as_df.reset_index(inplace=True)

        as_df.rename_axis(columns=None, inplace=True)

        as_df.rename(columns={'Category 1': 'ACCOUNT_STATUS_Active', 'Category 2': 'ACCOUNT_STATUS_Closed',
                              'Category 3': 'ACCOUNT_STATUS_Others'}, inplace=True)

        return as_df

    

    contributor_categories_df = contributor_categories(data2)
    #data1->Model Base   data2-> Historical Data
    data1 = pd.merge(data1, contributor_categories_df, on='CREDT_RPT_ID', how='left')
    #print('After contributer_categories:', data1.shape)

    account_status_df = account_status(data2)
    #print('account_status_df:', account_status_df.shape)
    data1 = pd.merge(data1, account_status_df, on='CREDT_RPT_ID', how='left')
    #print('After account_status:', data1.shape)

    def customer_type(df_t1):
        """
        Aggregation based on customer type and return good/bad customer type through delinquency and   touching bucket through user input.
        Arguments: 1 dataframe
        Return: 1 dataframe
        """
        df_t1.drop_duplicates(inplace=True)

        def split_dpd_hist(x):
            if isinstance(x, str) and x != "":
                return [x[i:i + 3] for i in range(0, len(x), 3)]
            else:
                return []

        # Split the 'DPD___HIST' column into 36 separate columns and reverse the order
        df_t1['DPD___HIST'] = df_t1['DPD___HIST'].apply(split_dpd_hist)

        # Reverse the order of values in each list
        df_t1['DPD___HIST'] = df_t1['DPD___HIST'].apply(lambda x: x[::-1])

        # Create 36 new columns for each month
        for i in range(1, 37):
            df_t1[f'M{i}'] = df_t1['DPD___HIST'].apply(lambda x: x[i - 1] if i <= len(x) else '')

        # Replace "XXX" and "DDD" values with "000" in all columns
        for i in range(1, 37):
            df_t1[f'M{i}'] = df_t1[f'M{i}'].replace({"XXX": "000", "DDD": "000"})

        # Define a function to categorize the values into buckets
        def categorize_value(value):
            if value == "000":
                return 0
            elif value.isdigit() and 1 <= int(value) <= 30:
                return 1
            elif value.isdigit() and 31 <= int(value) <= 60:
                return 2
            elif value.isdigit() and 61 <= int(value) <= 90:
                return 3
            elif value.isdigit() and int(value) > 90:
                return 4
            return value  # Return the original value if it doesn't match any criteria

        # Apply the categorize_value function to each of the 36 columns
        for i in range(1, 37):
            column_name = f'M{i}'
            df_t1[column_name] = df_t1[column_name].apply(categorize_value)

        # Convert "month1" through "month36" columns to integers, handling non-numeric values
        for i in range(1, 37):
            column_name = f'M{i}'
            df_t1[column_name] = pd.to_numeric(df_t1[column_name], errors='coerce').fillna(0).astype(int)

        def find_first_delinquency(row):
            for i in range(1, int(confs['ROLLING_WINDOW']) + 1):
                column_name = f'M{i}'
                if int(row[column_name]) >= int(confs['BUCKET']):
                    return i
            return 0  # If no delinquency is found, return 0

        # Apply the find_first_delinquency function to each row to create the "first_delinquency" column
        df_t1['first_delinquency'] = df_t1.apply(find_first_delinquency, axis=1)

        # Create a new column 'Good/Bad' based on the condition
        df_t1['Good/Bad'] = np.where(df_t1['first_delinquency'] == 0, 'Good', 'Bad')

        # Convert 'Good' to 0 and 'Bad' to 1
        df_t1['Good/Bad'] = df_t1['Good/Bad'].replace({'Good': 0, 'Bad': 1})
        df_t1.rename(columns= {'Good/Bad' : 'Target'}, inplace = True)
        # df_t1.rename(columns = {'Good/Bad':'Target'}, inplace = True)
        #print('Customer_type is about to end')

        return df_t1
    
    def max_delinquency(df_t2):
        """
        Aggregation based on delinquency and return the maximum delinquency of each customer.
        Arguments: 1 dataframe
        Return: 1 dataframe
        """
        df_t2.drop_duplicates(inplace=True)
        #print('CREDT_RPT_ID:', len(df_t2['CREDT_RPT_ID'].unique()))
        #print('No of rows:', df_t2.shape)

        def split_dpd_hist(x):
            if isinstance(x, str) and x != "":
                return [x[i:i + 3] for i in range(0, len(x), 3)]
            else:
                return []

        # Split the 'DPD___HIST' column into 36 separate columns and reverse the order
        df_t2['DPD___HIST'] = df_t2['DPD___HIST'].apply(split_dpd_hist)
        df_t2['DPD___HIST'] = df_t2['DPD___HIST'].apply(lambda x: x[::-1])

        for i in range(1, 37):
            df_t2[f'M{i}'] = df_t2['DPD___HIST'].apply(lambda x: x[i - 1] if i <= len(x) else '')

        for i in range(1, 37):
            df_t2[f'M{i}'] = df_t2[f'M{i}'].replace({"XXX": "000", "DDD": "000"})

        def categorize_value(value):
            if value == "000":
                return 0
            elif value.isdigit() and 1 <= int(value) <= 30:
                return 1
            elif value.isdigit() and 31 <= int(value) <= 60:
                return 2
            elif value.isdigit() and 61 <= int(value) <= 90:
                return 3
            elif value.isdigit() and int(value) > 90:
                return 4
            return value

        for i in range(1, 37):
            column_name = f'M{i}'
            df_t2[column_name] = df_t2[column_name].apply(categorize_value)

        for i in range(1, 37):
            column_name = f'M{i}'
            df_t2[column_name] = pd.to_numeric(df_t2[column_name], errors='coerce').fillna(0).astype(int)

        def calculate_current_bucket(row):
            dpd_month_tb_considered = row['DPD_month_tb_considered']

            if pd.notna(dpd_month_tb_considered):
                for i in range(1, 37):
                    if dpd_month_tb_considered <= i:
                        return row[f'M{i}']
                else:
                    return row['M36']
            else:
                return pd.NA

        df_t2['CURRENT_BUCKET'] = df_t2.apply(calculate_current_bucket, axis=1)
        df_t2['CURRENT_BUCKET'] = df_t2['CURRENT_BUCKET'].replace('<NA>', np.nan)


        df_t2_max_delinquency = df_t2.groupby('CREDT_RPT_ID')['CURRENT_BUCKET'].max().reset_index()
        
        df_t2_max_delinquency.rename(columns={'CURRENT_BUCKET': 'MAX_DELINQUENCY'}, inplace=True)
        

        # df_t2['max_delinquency_status'] = df_t2['CURRENT_BUCKET'].apply(extract_max_delinquency_status)

        
        
        # df_t2_max_delq = df_t2.groupby(['CREDT_RPT_ID']).agg({
        #     'max_delinquency_status': 'max'
        # }).reset_index()
        

        #print('Max_delinquency function done')
        return df_t2_max_delinquency
    
    def aggregating_disbursed_and_current_balance(df_q2):
        """
        Aggregation based on Disbursed amount, current balance and return the maximum delinquency of each customer.
        Arguments: 1 dataframe
        Return: 1 dataframe
        """

        agg_df = pd.DataFrame(df_q2['CREDT_RPT_ID'].unique())
        agg_df.rename(columns={0:'CREDT_RPT_ID'},inplace=True)

        agg_total_disbursed = df_q2.groupby('CREDT_RPT_ID')['_DISBURSED_AMT_HIGH_CREDIT'].sum().reset_index()
        agg_total_current_bal = df_q2.groupby('CREDT_RPT_ID')['_CURRENT_BAL'].sum().reset_index()
        agg_df = pd.merge(agg_df, agg_total_disbursed, on='CREDT_RPT_ID', how='left')
        agg_df = pd.merge(agg_df, agg_total_current_bal, on='CREDT_RPT_ID', how='left')
        agg_df = agg_df.rename(columns={'_DISBURSED_AMT_HIGH_CREDIT': 'TOTAL_DISBURSED_AMT_HIGH_CREDIT', '_CURRENT_BAL': 'TOTAL_CURRENT_BAL'})
        active_accounts = df_q2[df_q2['ACCOUNT_STATUS'] == 'Active']
        agg_total_disbursed = active_accounts.groupby('CREDT_RPT_ID')['_DISBURSED_AMT_HIGH_CREDIT'].sum().reset_index()
        agg_total_current_bal = active_accounts.groupby('CREDT_RPT_ID')['_CURRENT_BAL'].sum().reset_index()
        agg_df = pd.merge(agg_df, agg_total_disbursed, on='CREDT_RPT_ID', how='left')
        agg_df = pd.merge(agg_df, agg_total_current_bal, on='CREDT_RPT_ID', how='left')
        agg_df = agg_df.rename(columns={'_DISBURSED_AMT_HIGH_CREDIT': 'Active_TOTAL_DISBURSED_AMT_HIGH_CREDIT', '_CURRENT_BAL': 'Active_TOTAL_CURRENT_BAL'})

        return agg_df

########################################################################################################     
    #print('Before Customer_type:', data1.shape)
    data1 = customer_type(data1)
    #print('After Customer_type:', data1.shape)
    
    max_delinquency_df = max_delinquency(data2)
    logger(max_delinquency_df.shape)
    
    #print('Before joining max_delinquency:', data1.shape)
    data1 = pd.merge(data1, max_delinquency_df, on='CREDT_RPT_ID', how='left')
    data1.fillna(-1, inplace=True)
    #print('After joining max_delinquency:', data1.shape)
    #data1 = pd.merge(data1, customer_type_df, on='CREDT_RPT_ID', how='left')


    #print('Before going into Rockys code row number in data2', data2.shape)
    aggregate_df = aggregating_disbursed_and_current_balance(data2)
    #print('After coming out from Rocky code:', aggregate_df.shape)
    
    data1 = pd.merge(data1, aggregate_df, on='CREDT_RPT_ID', how='left')
    
    data1 = pd.merge(data1, product_type_df, on='CREDT_RPT_ID', how='left')
    print('Atanus code:', data1.columns)
    
    columns_to_drop = [f'M{i}' for i in range(37)]
    additional_columns = ['LOS_APP_ID', 'CANDIDATE___ID', 'CUSTOMER_ID_MBR_ID', 'BRANCH', 'KENDRA', 'SELF_INDICATOR', 'MATCH_TYPE', 'ACC_NUM', 'CREDIT_GRANTOR', 'ACCT_TYPE', 'CONTRIBUTOR_TYPE', 'DATE_REPORTED', 'OWNERSHIP_IND', 'ACCOUNT_STATUS', 'DISBURSED_DT', 'CLOSE_DT', 'LAST_PAYMENT_DATE', 'CREDIT_LIMIT_SANC_AMT', '_INSTALLMENT_AMT', 'INSTALLMENT_FREQUENCY', 'WRITE_OFF_DATE', '_OVERDUE_AMT', '_WRITE_OFF_AMT', 'ASSET_CLASS', '_ACCOUNT_REMARKS', 'LINKED_ACCOUNTS', 'REPORTED_DATE___HIST_', 'DPD___HIST', 'ASSET_CLASS___HIST_', 'HIGH_CRD___HIST_', 'CUR_BAL___HIST_', 'DAS___HIST_', 'AMT_OVERDUE___HIST_', 'AMT_PAID___HIST_', 'Unnamed__41', 'rn', 'PHONE_1', 'CREDT_RPT_ID_1']

    columns_to_drop.extend(additional_columns)

    data1 = data1.drop(columns=columns_to_drop, errors='ignore')
    
    data1 = pd.merge(data1, data3, on='CREDT_RPT_ID', how='left')
    
    data1['Number of Inquiry'].replace(np.nan, 0, inplace=True)
    
    #print('After merging from Rocky code:', data1.shape)
    return data1


##################################################################################################

# def snake_case_and_uppercase(column_name):
#     return column_name.replace(' ', '_').upper()

merged_data = merged_data_func()
# merged_data.columns = merged_data.columns.map(snake_case_and_uppercase())
merged_data.columns = merged_data.columns.str.upper()
merged_data.columns = merged_data.columns.str.replace(' ', '_')

df = merged_data.copy()
def replace_tenure_null(df, TENURE_, TENURE):
    df[TENURE_].fillna(df[TENURE], inplace=True)
    return df

df = replace_tenure_null(df, 'TENURE_', 'TENURE')

logger(df.columns)
columns_to_fill = [
    '_CURRENT_BAL',
    'CONTRIBUTOR_TYPE_NBF_CCC',
    'CONTRIBUTOR_TYPE_COP_OFI_HFC_RRB_MFI',
    'CONTRIBUTOR_TYPE_FRB_NAB_PRB_SFB',
    'CONTRIBUTOR_TYPE_ARC',
    'ACCOUNT_STATUS_ACTIVE',
    'ACCOUNT_STATUS_CLOSED',
    'ACCOUNT_STATUS_OTHERS',
    'TOTAL_DISBURSED_AMT_HIGH_CREDIT',
    'TOTAL_CURRENT_BAL',
    'ACTIVE_TOTAL_DISBURSED_AMT_HIGH_CREDIT',
    'ACTIVE_TOTAL_CURRENT_BAL'
]

df[columns_to_fill] = df[columns_to_fill].fillna(0)

additional_columns = ['_INCOME_INDICATOR_', '_OCCUPATION', 'CREATED_DATE', 'CUSTOMER_ID', 'TENURE', 'PHONE', 'RESIDENCE_TYPE', 'DESIRED_LOAN_AMOUNT', 'FIRST_DELINQUENCY']

df = df.drop(columns=additional_columns, errors='ignore')

logger('Initiating Merged function')
df.to_csv('{}_Training_Data.csv'.format(confs['PRODUCT_TYPE']), index=False)
logger(df.shape)
#print('Final columns:', list(merged_data.columns))
logger('Successfully created the Training data')#Training data

###############################################################################
logger('Starting modeling')

df_pl=df.copy()

# filtered data frame based on _DISBURSED_AMT_HIGH_CREDIT
df_pl = df_pl [(df_pl['_DISBURSED_AMT_HIGH_CREDIT']>= 50000) & (df_pl['_DISBURSED_AMT_HIGH_CREDIT'] <= 200000)]

# Create a new column 'TOTAL_ACCOUNTS' by summing up the values in the specified columns
df_pl['TOTAL_ACCOUNTS'] = df_pl['ACCOUNT_STATUS_ACTIVE'] + df_pl['ACCOUNT_STATUS_CLOSED'] + df_pl['ACCOUNT_STATUS_OTHERS']

# Create a new column 'TOTAL_ACCOUNTS' by summing up the values in the specified columns
df_pl['TOTAL_ACTIVE_ACCOUNTS'] = df_pl['NO_OF_ACTIVE_AUTO_LOAN'] + df_pl['NO_OF_ACTIVE_CREDIT_CARD'] + df_pl['NO_OF_ACTIVE_HOME_LOAN'] + df_pl['NO_OF_ACTIVE_HOME_LOAN'] + df_pl['NO_OF_ACTIVE_UNKNOWN']

exec(open('k2_iv_woe_function.py').read())
info_val = iv(df = df_pl.iloc[:, 1:], target = 'TARGET')
print(info_val)

my_logit_1 = sm.glm(
formula = """TARGET ~ _DISBURSED_AMT_HIGH_CREDIT+_CURRENT_BAL+INCOME_+TENURE_+
    INCOME+CITY_TIER+EMI_AMOUNT+CONTRIBUTOR_TYPE_NBF_CCC+CONTRIBUTOR_TYPE_COP_OFI_HFC_RRB_MFI+
    CONTRIBUTOR_TYPE_FRB_NAB_PRB_SFB+CONTRIBUTOR_TYPE_ARC+ACCOUNT_STATUS_ACTIVE+ACCOUNT_STATUS_CLOSED+
    ACCOUNT_STATUS_OTHERS+MAX_DELINQUENCY+TOTAL_DISBURSED_AMT_HIGH_CREDIT+TOTAL_CURRENT_BAL
    +ACTIVE_TOTAL_DISBURSED_AMT_HIGH_CREDIT+ACTIVE_TOTAL_CURRENT_BAL+NO_OF_ACTIVE_AUTO_LOAN+NO_OF_ACTIVE_CREDIT_CARD+NO_OF_ACTIVE_HOME_LOAN+
    NO_OF_ACTIVE_PERSONAL_LOAN+NO_OF_ACTIVE_UNKNOWN+NO_OF_CLOSED_AUTO_LOAN+NO_OF_CLOSED_CREDIT_CARD+NO_OF_CLOSED_HOME_LOAN+
    NO_OF_CLOSED_PERSONAL_LOAN+NO_OF_CLOSED_UNKNOWN+NO_OF_DELINQUENT_AUTO_LOAN+NO_OF_DELINQUENT_CREDIT_CARD+NO_OF_DELINQUENT_HOME_LOAN+
    NO_OF_DELINQUENT_PERSONAL_LOAN+NO_OF_OTHER_AUTO_LOAN+NO_OF_OTHER_CREDIT_CARD+NO_OF_OTHER_HOME_LOAN+NO_OF_OTHER_PERSONAL_LOAN+
    NO_OF_POST_WRITE_OFF_CLOSED_PERSONAL_LOAN+NO_OF_RESTRUCTURED__AND__CLOSED_PERSONAL_LOAN+NO_OF_SECURED+NO_OF_UNKNOWN+
    NO_OF_UNSECURED+MAX_DISBURSED_AMOUNT_AUTO_LOAN+MAX_DISBURSED_AMOUNT_CREDIT_CARD+MAX_DISBURSED_AMOUNT_HOME_LOAN+
    MAX_DISBURSED_AMOUNT_PERSONAL_LOAN+MAX_DISBURSED_AMOUNT_UNKNOWN+MAX_DISBURSED_AMT_HIGH_CREDIT+NUMBER_OF_INQUIRY+TOTAL_ACCOUNTS""", data = df_pl,
family = sma.families.Binomial()
).fit()
my_logit_1.summary()


def VIF(formula,data):
    y , X = dmatrices(formula,data = data,return_type="dataframe")
    vif = pd.DataFrame()
    vif["Var_Name"] = X.columns
    vif["VIF"] = [variance_inflation_factor(X.values, i) \
       for i in range(X.shape[1])]
    return(vif.round(1))

vif_check =VIF("""TARGET ~ _DISBURSED_AMT_HIGH_CREDIT+ACTIVE_TOTAL_CURRENT_BAL+ACCOUNT_STATUS_OTHERS+
               CONTRIBUTOR_TYPE_NBF_CCC+CONTRIBUTOR_TYPE_FRB_NAB_PRB_SFB+ACCOUNT_STATUS_ACTIVE+MAX_DELINQUENCY+
               ACTIVE_TOTAL_DISBURSED_AMT_HIGH_CREDIT+NO_OF_ACTIVE_CREDIT_CARD+NO_OF_ACTIVE_PERSONAL_LOAN+NO_OF_CLOSED_AUTO_LOAN+
               NO_OF_CLOSED_CREDIT_CARD+NO_OF_DELINQUENT_PERSONAL_LOAN+NO_OF_OTHER_PERSONAL_LOAN+NO_OF_UNSECURED""",
               data = df_pl)
print(vif_check)

# Calculate correlation matrix
correlation_matrix = df_pl.corr()

# Find highly correlated pairs (absolute correlation > 0.8, you can adjust this threshold)
high_corr_threshold = 0.8
high_corr_pairs = {}

# Iterate through the correlation matrix to find high correlations
for i in range(len(correlation_matrix.columns)):
    for j in range(i+1, len(correlation_matrix.columns)):
        if abs(correlation_matrix.iloc[i, j]) > high_corr_threshold:
            col1 = correlation_matrix.columns[i]
            col2 = correlation_matrix.columns[j]
            correlation_value = correlation_matrix.iloc[i, j]
            high_corr_pairs[(col1, col2)] = correlation_value

# Display highly correlated pairs and their correlation values
for pair, correlation in high_corr_pairs.items():
    print(f"Highly correlated columns: {pair} - Correlation: {correlation}")




# Calculate the correlation matrix
correlation_matrix = df_pl.corr()

# Create a heatmap
plt.figure(figsize=(40, 25))
sns.heatmap(correlation_matrix, cmap="coolwarm", annot=True, fmt=".2f", linewidths=.5)
plt.title("Correlation Heatmap")
plt.show()

import numpy as np

dev, val, holdout = np.split(
        df_pl.sample(frac=1, random_state=1212), 
        [int(.5*len(df_pl)), 
         int(.8*len(df_pl))]
        )

(len(dev), len(val), len(holdout))


print("Population Default Rate :", 
      round(sum(df_pl.TARGET)*100/len(df_pl),2),"%")
print("Development Sample Default Rate :", 
      round(sum(dev.TARGET)*100/len(dev),2),"%")
print("Validation Sample Default Rate :", 
      round(sum(val.TARGET)*100/len(val),2),"%")
print("Hold-out Sample Default Rate :", 
      round(sum(holdout.TARGET)*100/len(holdout),2),"%")


# Check for null values in each column
null_values = dev.isnull().sum()

# Display columns with null values and their counts
columns_with_null = null_values[null_values > 0]
print("Columns with Null Values:")
print(columns_with_null)

# Display the total count of null values in the DataFrame
total_null_count = null_values.sum()
print("\nTotal Null Values in the DataFrame:", total_null_count)


# Display rows where null values occur for the specified columns
rows_with_null = df_pl[df_pl[columns_with_null.index].isnull().any(axis=1)]
print("Rows with Null Values:")
rows_with_null.head()

# Drop rows with null values in the specified columns
dev = dev.dropna(subset=columns_with_null.index)

# Verify that null values are removed
print("Null values after dropping rows:", dev.isnull().sum().sum())

# Iterate through each numerical column and calculate UCL and LCL
for column in dev.select_dtypes(include='number'):
    Q1, Q3 = dev[column].quantile([0.25, 0.75])
    IQR = Q3 - Q1
    UCL = Q3 + 1.5 * IQR
    LCL = Q1 - 1.5 * IQR
    
    print(f"UCL for {column} = {round(UCL)}")
    print(f"LCL for {column} = {round(LCL)}")

# Create a list of columns to exclude from mapping values greater than UCL
exclude_columns = ['_DISBURSED_AMT_HIGH_CREDIT', 'MAX_DELINQUENCY', 'TARGET']

# Iterate through each numerical column and map values greater than UCL to UCL
for column in dev.select_dtypes(include='number'):
    if column not in exclude_columns:
        Q1, Q3 = dev[column].quantile([0.25, 0.75])
        UCL = Q3 + 1.5 * (Q3 - Q1)
        dev[column + '_OT'] = dev[column].apply(lambda x: UCL if x > UCL else x)


# Update values in the specified column
dev['ACCOUNT_STATUS_OTHERS_OT'] = dev['ACCOUNT_STATUS_OTHERS_OT'].replace(-1, 1)
dev['CONTRIBUTOR_TYPE_FRB_NAB_PRB_SFB_OT'] = dev['CONTRIBUTOR_TYPE_FRB_NAB_PRB_SFB_OT'].map(lambda X : 12 if  X >12 else X)
dev['ACTIVE_TOTAL_CURRENT_BAL_OT'] = dev['ACTIVE_TOTAL_CURRENT_BAL_OT'].map(lambda X : 0 if  X < 0 else X)
dev['ACTIVE_TOTAL_CURRENT_BAL_LOG'] = np.log(dev['ACTIVE_TOTAL_CURRENT_BAL_OT'] +1)
dev['NO_OF_ACTIVE_PERSONAL_LOAN_OT'] = dev['NO_OF_ACTIVE_PERSONAL_LOAN_OT'].map(lambda X : 7 if  X > 7 else X )




# Convert selected columns to string
columns_to_convert = ["NO_OF_CLOSED_AUTO_LOAN_OT", "NO_OF_CLOSED_CREDIT_CARD_OT", "NO_OF_OTHER_PERSONAL_LOAN_OT",
                      "NO_OF_OTHER_CREDIT_CARD_OT", "NUMBER_OF_INQUIRY_OT", "NO_OF_ACTIVE_PERSONAL_LOAN_OT"]

dev[columns_to_convert] = dev[columns_to_convert].astype(str)

# List of categorical columns for which you want to calculate WoE
categorical_columns = ['PROFESSION_TYPE', 'LOAN_PURPOSE', 'COMPANY_TYPE', 'QUALIFICATION', 'EXISTING_EMI',
                       'STATE', "NO_OF_CLOSED_AUTO_LOAN_OT", "NO_OF_CLOSED_CREDIT_CARD_OT",
                       "NO_OF_OTHER_PERSONAL_LOAN_OT", "NO_OF_OTHER_CREDIT_CARD_OT", "NUMBER_OF_INQUIRY_OT",
                       "NO_OF_ACTIVE_PERSONAL_LOAN_OT"]

# Specify your target variable name
target_variable = 'TARGET'  # Adjusted to match your y_train column name

# Iterate through each column (feature) in your DataFrame to calculate the WOE values
all_woe_map = {}

# Apply WoE transformation to each categorical column
for var in categorical_columns:
    woe_values = woe(dev, target_variable, var)
    woe_dict = dict(zip(woe_values[var], woe_values['WOE']))
    dev[var + '_WOE'] = dev[var].map(woe_dict)
    all_woe_map[var] = woe_dict

# Display the DataFrame with added WOE columns
woe_transformed_data = dev.copy()  # Assuming you want to create a new DataFrame
woe_transformed_data.head()

# Save the WOE mappings to a pickle file
with open("woe_mapping_2.pkl", "wb") as f:
    pickle.dump(all_woe_map, f)

# Display the updated DataFrame with WoE columns
dev.head()


def VIF(formula,data):
    y , X = dmatrices(formula,data = data,return_type="dataframe")
    vif = pd.DataFrame()
    vif["Var_Name"] = X.columns
    vif["VIF"] = [variance_inflation_factor(X.values, i) \
       for i in range(X.shape[1])]
    return(vif.round(1))

vif_check =VIF("""TARGET ~ ACCOUNT_STATUS_OTHERS_OT + CONTRIBUTOR_TYPE_FRB_NAB_PRB_SFB_OT + ACTIVE_TOTAL_CURRENT_BAL_OT+
            MAX_DELINQUENCY + NO_OF_ACTIVE_PERSONAL_LOAN_OT_WOE + NO_OF_CLOSED_AUTO_LOAN_OT_WOE +
            NO_OF_CLOSED_CREDIT_CARD_OT_WOE +NO_OF_OTHER_PERSONAL_LOAN_OT_WOE +
            NO_OF_OTHER_CREDIT_CARD_OT_WOE + NUMBER_OF_INQUIRY_OT_WOE""",
               data = dev)
vif_check

import statsmodels.formula.api as sm
import statsmodels.api as sma

my_logit_2 = sm.glm(
formula = """TARGET ~ ACCOUNT_STATUS_OTHERS_OT + CONTRIBUTOR_TYPE_FRB_NAB_PRB_SFB_OT + ACTIVE_TOTAL_CURRENT_BAL_OT+
            MAX_DELINQUENCY + NO_OF_ACTIVE_PERSONAL_LOAN_OT_WOE + NO_OF_CLOSED_AUTO_LOAN_OT_WOE +
            NO_OF_CLOSED_CREDIT_CARD_OT_WOE +NO_OF_OTHER_PERSONAL_LOAN_OT_WOE +
            NO_OF_OTHER_CREDIT_CARD_OT_WOE + NUMBER_OF_INQUIRY_OT_WOE""", data = dev,
family = sma.families.Binomial()
).fit()
my_logit_2.summary()


import joblib

# Save the logistic regression model using joblib
joblib.dump(my_logit_2, 'my_logit_2.joblib')

dev.columns


## Predicting Probabilities
dev["prob"] = my_logit_2.predict(dev)


dev.head()


dev['decile']=pd.qcut(dev.prob, 10, labels=False)

def Rank_Order(X,y,Target):
    
    Rank=X.groupby('decile').apply(lambda x: pd.Series([
        np.min(x[y]),
        np.max(x[y]),
        np.mean(x[y]),
        np.size(x[y]),
        np.sum(x[Target]),
        np.size(x[Target][x[Target]==0]),
        ],
        index=(["min_prob","max_prob","avg_prob",
                "cnt_cust","cnt_resp","cnt_non_resp"])
        )).reset_index()
    Rank=Rank.sort_values(by='decile',ascending=False)
    Rank["rrate"]=round(Rank["cnt_resp"]*100/Rank["cnt_cust"],2)
    Rank["cum_cust"]=np.cumsum(Rank["cnt_cust"])
    Rank["cum_resp"]=np.cumsum(Rank["cnt_resp"])
    Rank["cum_non_resp"]=np.cumsum(Rank["cnt_non_resp"])
    Rank["cum_cust_pct"]=round(Rank["cum_cust"]*100/np.sum(Rank["cnt_cust"]),2)
    Rank["cum_resp_pct"]=round(Rank["cum_resp"]*100/np.sum(Rank["cnt_resp"]),2)
    Rank["cum_non_resp_pct"]=round(
            Rank["cum_non_resp"]*100/np.sum(Rank["cnt_non_resp"]),2)
    Rank["KS"] = round(Rank["cum_resp_pct"] - Rank["cum_non_resp_pct"],2)
    Rank["Lift"] = round(Rank["cum_resp_pct"] / Rank["cum_cust_pct"],2)
    Rank
    return(Rank)


Gains_Table = Rank_Order(dev,"prob","TARGET")
print(Gains_Table)


from sklearn.metrics import roc_curve
import matplotlib.pyplot as plt
fpr, tpr, thresholds = roc_curve(dev["TARGET"],dev["prob"] )


from sklearn.metrics import  auc
roc_auc = round(auc(fpr, tpr), 3)

KS = round((tpr - fpr).max(), 5)

print("AUC of the model is:", roc_auc)
print("KS of the model is:", KS)


gini_coeff = 2 * roc_auc - 1
print("Gini Coefficient of the model is:", round(gini_coeff,3))

from sklearn.metrics import confusion_matrix

# Assuming a threshold of 0.5 for binary classification
predictions_binary = (dev["prob"] > 0.5).astype(int)
conf_matrix = confusion_matrix(dev["TARGET"], predictions_binary)

print("Confusion Matrix:")
print(conf_matrix)

# Extracting TP, TN, FP, FN from the confusion matrix
TP = conf_matrix[1, 1]
TN = conf_matrix[0, 0]
FP = conf_matrix[0, 1]
FN = conf_matrix[1, 0]

# Calculating accuracy
accuracy = (TP + TN) / (TP + TN + FP + FN)

print("Accuracy of the model is:", round(accuracy, 3))
logger('predict this model on validation dataset')

# Drop rows with null values in the specified columns
val = val.dropna(subset=columns_with_null.index)

# Verify that null values are removed
print("Null values after dropping rows:", val.isnull().sum().sum())

# Create a list of columns to exclude from mapping values greater than UCL
exclude_columns = ['_DISBURSED_AMT_HIGH_CREDIT', 'MAX_DELINQUENCY', 'TARGET']

# Iterate through each numerical column and map values greater than UCL to UCL
for column in val.select_dtypes(include='number'):
    if column not in exclude_columns:
        Q1, Q3 = dev[column].quantile([0.25, 0.75])
        UCL = Q3 + 1.5 * (Q3 - Q1)
        val[column + '_OT'] = val[column].apply(lambda x: UCL if x > UCL else x)       

# Update values in the specified column
val['ACCOUNT_STATUS_OTHERS_OT'] = val['ACCOUNT_STATUS_OTHERS_OT'].replace(-1, 1)
val['CONTRIBUTOR_TYPE_FRB_NAB_PRB_SFB_OT'] = val['CONTRIBUTOR_TYPE_FRB_NAB_PRB_SFB_OT'].map(lambda X : 12 if  X >12 else X)
val['ACTIVE_TOTAL_CURRENT_BAL_OT'] = val['ACTIVE_TOTAL_CURRENT_BAL_OT'].map(lambda X : 0 if  X < 0 else X)
val['ACTIVE_TOTAL_CURRENT_BAL_LOG'] = np.log(val['ACTIVE_TOTAL_CURRENT_BAL_OT'] +1)
val['NO_OF_ACTIVE_PERSONAL_LOAN_OT'] = val['NO_OF_ACTIVE_PERSONAL_LOAN_OT'].map(lambda X : 7 if  X > 7 else X )



# Convert selected columns to string
columns_to_convert = ["NO_OF_CLOSED_AUTO_LOAN_OT", "NO_OF_CLOSED_CREDIT_CARD_OT", "NO_OF_OTHER_PERSONAL_LOAN_OT",
                      "NO_OF_OTHER_CREDIT_CARD_OT", "NUMBER_OF_INQUIRY_OT", "NO_OF_ACTIVE_PERSONAL_LOAN_OT"]

val[columns_to_convert] = val[columns_to_convert].astype(str)

# List of categorical columns for which you want to calculate WoE
categorical_columns = ['PROFESSION_TYPE', 'LOAN_PURPOSE', 'COMPANY_TYPE', 'QUALIFICATION', 'EXISTING_EMI',
                       'STATE', "NO_OF_CLOSED_AUTO_LOAN_OT", "NO_OF_CLOSED_CREDIT_CARD_OT",
                       "NO_OF_OTHER_PERSONAL_LOAN_OT", "NO_OF_OTHER_CREDIT_CARD_OT", "NUMBER_OF_INQUIRY_OT",
                       "NO_OF_ACTIVE_PERSONAL_LOAN_OT"]

# Load the WOE mapping from the pickle file
with open("woe_mapping_2.pkl", "rb") as f:
    woe_mapping = pickle.load(f)

# Map the WOE values to the validation data
for column in categorical_columns:
    woe_values_dict = woe_mapping.get(column, {})
    val[f"{column}_WOE"] = val[column].map(woe_values_dict)


val["prob_2"] = my_logit_2.predict(val)

val['decile']=pd.qcut(val.prob_2, 10, labels=False)

def Rank_Order(X,y,Target):
    
    Rank=X.groupby('decile').apply(lambda x: pd.Series([
        np.min(x[y]),
        np.max(x[y]),
        np.mean(x[y]),
        np.size(x[y]),
        np.sum(x[Target]),
        np.size(x[Target][x[Target]==0]),
        ],
        index=(["min_prob","max_prob","avg_prob",
                "cnt_cust","cnt_resp","cnt_non_resp"])
        )).reset_index()
    Rank=Rank.sort_values(by='decile',ascending=False)
    Rank["rrate"]=round(Rank["cnt_resp"]*100/Rank["cnt_cust"],2)
    Rank["cum_cust"]=np.cumsum(Rank["cnt_cust"])
    Rank["cum_resp"]=np.cumsum(Rank["cnt_resp"])
    Rank["cum_non_resp"]=np.cumsum(Rank["cnt_non_resp"])
    Rank["cum_cust_pct"]=round(Rank["cum_cust"]*100/np.sum(Rank["cnt_cust"]),2)
    Rank["cum_resp_pct"]=round(Rank["cum_resp"]*100/np.sum(Rank["cnt_resp"]),2)
    Rank["cum_non_resp_pct"]=round(
            Rank["cum_non_resp"]*100/np.sum(Rank["cnt_non_resp"]),2)
    Rank["KS"] = round(Rank["cum_resp_pct"] - Rank["cum_non_resp_pct"],2)
    Rank["Lift"] = round(Rank["cum_resp_pct"] / Rank["cum_cust_pct"],2)
    Rank
    return(Rank)


Gains_Table = Rank_Order(val,"prob_2","TARGET")
print(Gains_Table)





from sklearn.metrics import  auc
roc_auc = round(auc(fpr, tpr), 3)

KS = round((tpr - fpr).max(), 5)

print("AUC of the model is:", roc_auc)
print("KS of the model is:", KS)

gini_coeff = 2 * roc_auc - 1
print("Gini Coefficient of the model is:", round(gini_coeff,3))

from sklearn.metrics import confusion_matrix

# Assuming a threshold of 0.5 for binary classification
predictions_binary = (val["prob_2"] > 0.5).astype(int)
conf_matrix = confusion_matrix(val["TARGET"], predictions_binary)

print("Confusion Matrix:")
print(conf_matrix)

# Extracting TP, TN, FP, FN from the confusion matrix
TP = conf_matrix[1, 1]
TN = conf_matrix[0, 0]
FP = conf_matrix[0, 1]
FN = conf_matrix[1, 0]

accuracy = (TP + TN) / (TP + TN + FP + FN)
precision=TP/(TP+FP)
recall=TP/(TP+FN)
print("Accuracy of the model is:", round(accuracy, 3))
print("Precision of the model is:", round(precision, 2))
print("Recall of the model is:", round(recall, 2))


# Assuming you have a log-odds variable named log_odds and predicted probabilities named prob
log_odds = np.log(val["prob_2"] / (1 - val["prob_2"]))


from sklearn.metrics import roc_curve, auc

# Set up parameters for scorecard
pdo = 20
factor = pdo / np.log(2)
offset = 600-factor * log_odds

# Calculate scores using the log-odds, factor, and offset
scores = -(factor * log_odds) + offset
scores = np.round(scores).astype(int)

# Define score ranges and labels
score_ranges = [300, 450, 550, 650, 800, 900]
score_labels = ['Very Low', 'Low', 'Good', 'Very Good', 'Excellent']

# Create a DataFrame to store the credit scores and categories
credit_score_df = pd.DataFrame({'Roopya Score': scores})

credit_score_df['Category'] = pd.cut(credit_score_df['Roopya Score'], bins=score_ranges, labels=score_labels, right=False)


# Assuming holdout is your test dataset, merge DataFrames based on the index
scoring_df_val = pd.merge(val, credit_score_df, left_index=True, right_index=True)

# Display the first 50 rows of the scoring DataFrame
print(scoring_df_val.head(10))


roopya_scoring_df = scoring_df_val[['CREDT_RPT_ID','prob_2', 'Roopya Score','Category']]


roopya_scoring_df['Category'].value_counts()
logger('model building on holdout dataset')

# Drop rows with null holdoutues in the specified columns
holdout = holdout.dropna(subset=columns_with_null.index)

# Verify that null holdoutues are removed
print("Null holdoutues after dropping rows:", holdout.isnull().sum().sum())

# Create a list of columns to exclude from mapping holdoutues greater than UCL
exclude_columns = ['_DISBURSED_AMT_HIGH_CREDIT', 'MAX_DELINQUENCY', 'TARGET']



# Iterate through each numerical column and map holdoutues greater than UCL to UCL
for column in holdout.select_dtypes(include='number'):
    if column not in exclude_columns:
        Q1, Q3 = dev[column].quantile([0.25, 0.75])
        UCL = Q3 + 1.5 * (Q3 - Q1)
        holdout[column + '_OT'] = holdout[column].apply(lambda x: UCL if x > UCL else x)       

# Update holdoutues in the specified column
holdout['ACCOUNT_STATUS_OTHERS_OT'] = holdout['ACCOUNT_STATUS_OTHERS_OT'].replace(-1, 1)
holdout['CONTRIBUTOR_TYPE_FRB_NAB_PRB_SFB_OT'] = holdout['CONTRIBUTOR_TYPE_FRB_NAB_PRB_SFB_OT'].map(lambda X : 12 if  X >12 else X)
holdout['ACTIVE_TOTAL_CURRENT_BAL_OT'] = holdout['ACTIVE_TOTAL_CURRENT_BAL_OT'].map(lambda X : 0 if  X < 0 else X)
holdout['ACTIVE_TOTAL_CURRENT_BAL_LOG'] = np.log(holdout['ACTIVE_TOTAL_CURRENT_BAL_OT'] +1)
holdout['NO_OF_ACTIVE_PERSONAL_LOAN_OT'] = holdout['NO_OF_ACTIVE_PERSONAL_LOAN_OT'].map(lambda X : 7 if  X > 7 else X )



# Convert selected columns to string
columns_to_convert = ["NO_OF_CLOSED_AUTO_LOAN_OT", "NO_OF_CLOSED_CREDIT_CARD_OT", "NO_OF_OTHER_PERSONAL_LOAN_OT",
                      "NO_OF_OTHER_CREDIT_CARD_OT", "NUMBER_OF_INQUIRY_OT", "NO_OF_ACTIVE_PERSONAL_LOAN_OT"]

holdout[columns_to_convert] = holdout[columns_to_convert].astype(str)

# List of categorical columns for which you want to calculate WoE
categorical_columns = ['PROFESSION_TYPE', 'LOAN_PURPOSE', 'COMPANY_TYPE', 'QUALIFICATION', 'EXISTING_EMI',
                       'STATE', "NO_OF_CLOSED_AUTO_LOAN_OT", "NO_OF_CLOSED_CREDIT_CARD_OT",
                       "NO_OF_OTHER_PERSONAL_LOAN_OT", "NO_OF_OTHER_CREDIT_CARD_OT", "NUMBER_OF_INQUIRY_OT",
                       "NO_OF_ACTIVE_PERSONAL_LOAN_OT"]

# Load the WOE mapping from the pickle file
with open("woe_mapping_2.pkl", "rb") as f:
    woe_mapping = pickle.load(f)

# Map the WOE holdoutues to the holdoutidation data
for column in categorical_columns:
    woe_holdoutues_dict = woe_mapping.get(column, {})
    holdout[f"{column}_WOE"] = holdout[column].map(woe_holdoutues_dict)


# Display the updated DataFrame with WoE columns
holdout.head()

    
    
    

import joblib

# Load the saved logistic regression model using joblib
loaded_model = joblib.load('my_logit_2.joblib')

predictions = loaded_model.predict(holdout)

# You can also print the summary of the loaded model
print(loaded_model.summary())

holdout["prob_3"] = loaded_model.predict(holdout)

holdout.head()

holdout['decile']=pd.qcut(holdout.prob_3, 10, labels=False)

def Rank_Order(X,y,Target):
    
    Rank=X.groupby('decile').apply(lambda x: pd.Series([
        np.min(x[y]),
        np.max(x[y]),
        np.mean(x[y]),
        np.size(x[y]),
        np.sum(x[Target]),
        np.size(x[Target][x[Target]==0]),
        ],
        index=(["min_prob","max_prob","avg_prob",
                "cnt_cust","cnt_resp","cnt_non_resp"])
        )).reset_index()
    Rank=Rank.sort_values(by='decile',ascending=False)
    Rank["rrate"]=round(Rank["cnt_resp"]*100/Rank["cnt_cust"],2)
    Rank["cum_cust"]=np.cumsum(Rank["cnt_cust"])
    Rank["cum_resp"]=np.cumsum(Rank["cnt_resp"])
    Rank["cum_non_resp"]=np.cumsum(Rank["cnt_non_resp"])
    Rank["cum_cust_pct"]=round(Rank["cum_cust"]*100/np.sum(Rank["cnt_cust"]),2)
    Rank["cum_resp_pct"]=round(Rank["cum_resp"]*100/np.sum(Rank["cnt_resp"]),2)
    Rank["cum_non_resp_pct"]=round(
            Rank["cum_non_resp"]*100/np.sum(Rank["cnt_non_resp"]),2)
    Rank["KS"] = round(Rank["cum_resp_pct"] - Rank["cum_non_resp_pct"],2)
    Rank["Lift"] = round(Rank["cum_resp_pct"] / Rank["cum_cust_pct"],2)
    Rank
    return(Rank)


Gains_Table = Rank_Order(holdout,"prob_3","TARGET")
print(Gains_Table)

from sklearn.metrics import  auc
roc_auc = round(auc(fpr, tpr), 3)

KS = round((tpr - fpr).max(), 5)

print("AUC of the model is:", roc_auc)
print("KS of the model is:", KS)

gini_coeff = 2 * roc_auc - 1
print("Gini Coefficient of the model is:", round(gini_coeff,3))

# Assuming a threshold of 0.5 for binary classification
predictions_binary = (holdout["prob_3"] > 0.5).astype(int)
conf_matrix = confusion_matrix(holdout["TARGET"], predictions_binary)

print("Confusion Matrix:")
print(conf_matrix)

# Extracting TP, TN, FP, FN from the confusion matrix
TP = conf_matrix[1, 1]
TN = conf_matrix[0, 0]
FP = conf_matrix[0, 1]
FN = conf_matrix[1, 0]

# Calculating accuracy
accuracy = (TP + TN) / (TP + TN + FP + FN)
precision=TP/(TP+FP)
recall=TP/(TP+FN)
print("Accuracy of the model is:", round(accuracy, 3))
print("Precision of the model is:", round(precision, 2))
print("Recall of the model is:", round(recall, 2))

logger('Scorecard building started')

# Assuming you have a log-odds variable named log_odds and predicted probabilities named prob
log_odds = np.log(holdout["prob_3"] / (1 - holdout["prob_3"]))

from sklearn.metrics import roc_curve, auc

# Set up parameters for scorecard
pdo = 20
factor = pdo / np.log(2)
offset = 600-factor * log_odds

# Calculate scores using the log-odds, factor, and offset
scores = -(factor * log_odds) + offset
scores = np.round(scores).astype(int)

# Define score ranges and labels
score_ranges = [300, 450, 550, 650, 800, 900]
score_labels = ['Very Low', 'Low', 'Good', 'Very Good', 'Excellent']

# Create a DataFrame to store the credit scores and categories
credit_score_df = pd.DataFrame({'Roopya Score': scores})

credit_score_df['Category'] = pd.cut(credit_score_df['Roopya Score'], bins=score_ranges, labels=score_labels, right=False)


# Assuming holdout is your test dataset, merge DataFrames based on the index
scoring_df_holdout = pd.merge(holdout, credit_score_df, left_index=True, right_index=True)

roopya_scoring_df_holdout = scoring_df_holdout[['CREDT_RPT_ID','prob_3', 'Roopya Score','Category']]

print(roopya_scoring_df_holdout.head(30))

print(roopya_scoring_df_holdout.min())
print(roopya_scoring_df_holdout.max())
print(roopya_scoring_df_holdout['Category'].value_counts())

logger('Scorecard building completed')

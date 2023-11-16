#Testing data
from google.cloud import bigquery
from google.cloud import storage
from pandas_gbq import to_gbq
import pandas as pd
import numpy as np
import pickle

print('Hmmm')
confs = {}
Home_Loan = ['Housing Loan', 'Property Loan', 'Leasing', 'Microfinance Housing Loan']
Credit_Loan = ['Credit Card', 'Corporate Credit Card', 'Kisan Credit Card', 'Secured Credit Card', 'Loan on Credit Card']
Auto_Loan = ['Auto Loan (Personal)','Two-Wheeler Loan','Commercial Vehicle Loan','Used Car Loan','Used Tractor Loan','Tractor Loan']
Personal_Loan = ['Personal Loan', 'Overdraft', 'Consumer Loan', 'Loan Aganist Bank Deposits', 'OD on Savings Account', 'Microfinance Personal Loan', 'Loan to Professional']

def get_confs():
    global confs
    conf_df = pd.read_csv("confs.txt", delimiter='=', names=['KEY', 'VALUE'])
    confs = dict(zip(conf_df['KEY'], conf_df['VALUE']))
    return confs

def Loan_categories(confs):
    sub_types = ''
    if confs['PRODUCT_TYPE'] == 'Home Loan':
        for element in Home_Loan:
            sub_types = sub_types + "'" + element + "'" + ','
        sub_types = sub_types[:len(sub_types) - 1]
    elif confs['PRODUCT_TYPE'] == 'Auto Loan':
        for element in Auto_Loan:
            sub_types = sub_types + "'" + element + "'" + ','
        sub_types = sub_types[:len(sub_types) - 1]
    elif confs['PRODUCT_TYPE'] == 'Personal Loan':
        for element in Personal_Loan:
            sub_types = sub_types + "'" + element + "'" + ','
        sub_types = sub_types[:len(sub_types) - 1]
    elif confs['PRODUCT_TYPE'] == 'Credit Card':
        for element in Credit_Loan:
            sub_types = sub_types + "'" + element + "'" + ','
        sub_types = sub_types[:len(sub_types) - 1]
    else:
        print('Not a valid information')
    
    return sub_types

get_confs()
print(confs['PRODUCT_TYPE'])
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
    # Create a client using default credentials
    client = storage.Client()
    
#     df1 = pd.read_excel(f"{confs['RAW_APPLICATION_DATA']}")

#     print('Read successful!!!!!!')
    
#     file_path_w = '{}'.format(confs['APPLICATION_DATA'])
#     df1.to_csv(file_path_w, index = False, index_label = False)

    df2 = pd.read_csv(f"{confs['APPLICATION_DATA']}")
    #df2 = df2.head(10000)

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
    
    print(df1.columns)
    df1.to_csv('Untitled Folder/Raw_Cleaned_dataset.csv', index=False,index_label=False)
    df2 = pd.read_csv('Untitled Folder/Raw_Cleaned_dataset.csv')
    
    print('Almost there, hold on')
    #print(df2.head(5))
    
    client = bigquery.Client()
    project_id = f"{confs['PROJECT_ID']}"
    destination_table_id = f"{confs['DESTINATION_ID']}"

    to_gbq(df2, destination_table_id, project_id=project_id, if_exists='replace')
    return 0
print('Starting cleaning steps')
basic_cleaning_steps()
print('Successfully completed cleaning steps')


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
print('Initiating IOI function')
inquiry_data.to_csv('Untitled Folder/IOI_Data_{}.csv'.format(confs['PRODUCT_TYPE']), index=False)
#print('After historical_data:', historical_data.shape)
print('Initiation done')

def merged_data_func():
    """
        Reading the Model Base and Historical Data and performing categorization of contributor type, aggregation of account status, customer type, delinquency, disbursed and current balance so that later we can call this one function to get the training data.
        Arguments: NA
        Return: 1 dataframe
    """
    print('Initiating Swarnavos query')
    #data1 = pd.read_csv('Untitled Folder/Model_Base_{}.csv'.format(confs['PRODUCT_TYPE']))
    query_for_test_data = f"""WITH CTE1 AS (
    SELECT *
    FROM `{confs['PROJECT_ID']}.{confs['DESTINATION_ID']}` AS app
    LEFT JOIN `{confs['INQUIRY_TABLE']}` AS inq ON app.PHONE = inq.PHONE_1
)

SELECT *
FROM `{confs['ACCOUNT_TABLE']}` AS acc
LEFT JOIN CTE1 ON CTE1.CREDT_RPT_ID = acc.CREDT_RPT_ID
              WHERE DATE(CTE1.CREATED_DATE) > DATE(acc.DISBURSED_DT);"""
    
    print(query_for_test_data)
    query_job = client.query(query_for_test_data)
    results = query_job.result()

    data2 = results.to_dataframe()
    print('data2 shape',data2.shape)
    data2.columns = data2.columns.str.upper()
    data2.columns = data2.columns.str.replace(' ', '_')
    print('data2 shape',data2.columns)
    data2.drop(columns=['_DISBURSED_AMT_HIGH_CREDIT'],inplace=True)
    data2.rename(columns={'DESIRED_LOAN_AMOUNT':'_DISBURSED_AMT_HIGH_CREDIT'},inplace=True)
    df_temp = data2[['CREDT_RPT_ID', 'TENURE_', 'GENDER', 'MARITAL_STATUS', 'INCOME', 'CITY_TIER', 'PROFESSION_TYPE', 'TOTAL_WORK_EXPERIENCE', 'QUALIFICATION', 'EXISTING_EMI', 'EMI_AMOUNT', 'LOAN_PURPOSE','_DISBURSED_AMT_HIGH_CREDIT', 'TENURE']]
    df_temp.drop_duplicates(inplace=True)
    df=pd.DataFrame(data2['CREDT_RPT_ID'].unique())
    df.rename(columns={0:'CREDT_RPT_ID'},inplace=True)
    df = df.merge(df_temp, how='left', on='CREDT_RPT_ID')
    print('df shape',df.shape)
    print('Jio Kaka')
    
    
    data3 = pd.read_csv('Untitled Folder/IOI_Data_{}.csv'.format(confs['PRODUCT_TYPE']))
    
    def pivot_and_aggregation_of_inquiry(df_ioi):
        """
        Aggregation based on Inquiry type and return aggregated inquiry with unique Credit Report Id.
        Arguments: 1 dataframe
        Return: 1 dataframe
        """
        df_ioi_counts=pd.DataFrame(df_ioi['CREDT_RPT_ID'].value_counts())
        df_ioi_counts.reset_index(inplace=True)
        df_ioi_counts.rename(columns={"CREDT_RPT_ID":"Number of Inquiry","index":"CREDT_RPT_ID"},inplace=True)
        return df_ioi_counts
    
    #Call the pivot_and_aggregation_of_inquiry to perform action, will use this at the end.
    data3 = pivot_and_aggregation_of_inquiry(data3)
    

    def contributor_categories(df):
        """
        Categorization based on Contributor type and return contributor categories
        Arguments: 1 dataframe
        Return: 1 dataframe
        """
        print('Contributor')
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
        print('Account Status')
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
    
    def max_delinquency(df_t2):
        """
        Aggregation based on delinquency and return the maximum delinquency of each customer.
        Arguments: 1 dataframe
        Return: 1 dataframe
        """
        print('Max Deli')
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
        
        #print('Max_delinquency function done')
        return df_t2_max_delinquency
    
    def aggregating_disbursed_and_current_balance(df_q2):
        """
        Aggregation based on Disbursed amount, current balance and return the maximum delinquency of each customer.
        Arguments: 1 dataframe
        Return: 1 dataframe
        """
        print("-------Rocky Func start")
        agg_df = pd.DataFrame(df_q2['CREDT_RPT_ID'].unique())
        agg_df.rename(columns={0:'CREDT_RPT_ID'},inplace=True)
        print("-------Rocky Wrong",agg_df.shape)
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
        print("-------Rocky Return",agg_df.shape)
        return agg_df

######################################################################################################
    
    contributor_categories_df = contributor_categories(data2)
    contributor_categories_df.reset_index(inplace=True)
    df = pd.merge(df, contributor_categories_df, how='left', on='CREDT_RPT_ID')
    #print(contributor_categories_df.head())
    account_status_df = account_status(data2)
    account_status_df.reset_index(inplace=True)
    df = pd.merge(df, account_status_df, how='left', on='CREDT_RPT_ID')    
    #max_delinquency_df = max_delinquency(data2)
    #df = pd.merge(df, max_delinquency_df, how='left', on='CREDT_RPT_ID')
    #df.fillna(-1, inplace=True)
    #data2_concat = pd.merge(data2_concat, max_delinquency_df, how='inner', on='CREDT_RPT_ID')
    #print('After max_delinquency:', max_delinquency_df.shape)
    print(df.shape)
    #print('Before going into Rockys code', data1.shape)
    aggregate_df = aggregating_disbursed_and_current_balance(data2)
    df = pd.merge(df, aggregate_df, how='left', on='CREDT_RPT_ID')
    print(df.shape)
    #print('After coming out from Rocky code:', aggregate_df.shape)
    
    columns_to_drop = [f'M{i}' for i in range(37)]
    additional_columns = ['LOS_APP_ID', 'CANDIDATE___ID', 'CUSTOMER_ID_MBR_ID', 'BRANCH', 'KENDRA', 'SELF_INDICATOR', 'MATCH_TYPE', 'ACC_NUM', 'CREDIT_GRANTOR', 'ACCT_TYPE', 'CONTRIBUTOR_TYPE', 'DATE_REPORTED', 'OWNERSHIP_IND', 'ACCOUNT_STATUS', 'DISBURSED_DT', 'CLOSE_DT', 'LAST_PAYMENT_DATE', 'CREDIT_LIMIT_SANC_AMT', '_INSTALLMENT_AMT', 'INSTALLMENT_FREQUENCY', 'WRITE_OFF_DATE', '_OVERDUE_AMT', '_WRITE_OFF_AMT', 'ASSET_CLASS', '_ACCOUNT_REMARKS', 'LINKED_ACCOUNTS', 'REPORTED_DATE___HIST_', 'DPD___HIST', 'ASSET_CLASS___HIST_', 'HIGH_CRD___HIST_', 'CUR_BAL___HIST_', 'DAS___HIST_', 'AMT_OVERDUE___HIST_', 'AMT_PAID___HIST_', 'Unnamed__41', 'rn', 'PHONE_1', 'CREDT_RPT_ID_1']

    columns_to_drop.extend(additional_columns)

    df = df.drop(columns=columns_to_drop, errors='ignore')
    
    data1 = pd.merge(df, data3, on='CREDT_RPT_ID', how='left')
    
    data1['Number of Inquiry'].replace(np.nan, 0, inplace=True)
    
    #print('After merging from Rocky code:', data1.shape)
    return data1


##################################################################################################

merged_data = merged_data_func()
# merged_data.columns = merged_data.columns.map(snake_case_and_uppercase())
merged_data.columns = merged_data.columns.str.upper()
merged_data.columns = merged_data.columns.str.replace(' ', '_')

df = merged_data.copy()
def replace_tenure_null(df, TENURE_, TENURE):
    df[TENURE_].fillna(df[TENURE], inplace=True)
    return df

df = replace_tenure_null(df, 'TENURE_', 'TENURE')

columns_to_fill = [
    #'_CURRENT_BAL',
    'CONTRIBUTOR_TYPE_NBF_CCC',
    'CONTRIBUTOR_TYPE_COP_OFI_HFC_RRB_MFI',
    'CONTRIBUTOR_TYPE_FRB_NAB_PRB_SFB',
    #'CONTRIBUTOR_TYPE_ARC',
    'ACCOUNT_STATUS_ACTIVE',
    'ACCOUNT_STATUS_CLOSED',
    'ACCOUNT_STATUS_OTHERS',
   # 'MAX_DELINQUENCY_STATUS',
    'TOTAL_DISBURSED_AMT_HIGH_CREDIT',
    'TOTAL_CURRENT_BAL',
    'ACTIVE_TOTAL_DISBURSED_AMT_HIGH_CREDIT',
    'ACTIVE_TOTAL_CURRENT_BAL',
    'ACCOUNT_STATUS_ACTIVE',
    'ACCOUNT_STATUS_OTHERS'
]

#df[columns_to_fill] = df[columns_to_fill].fillna(0)
df[columns_to_fill].fillna(0,inplace=True)
additional_columns = ['_INCOME_INDICATOR_', '_OCCUPATION', 'CREATED_DATE', 'CUSTOMER_ID', 'TENURE', 'PHONE', 'RESIDENCE_TYPE', 'DESIRED_LOAN_AMOUNT', 'FIRST_DELINQUENCY', 'TOTAL_CURRENT_BAL']

#df = df.drop(columns=additional_columns, errors='ignore')

df = df[['CREDT_RPT_ID', '_DISBURSED_AMT_HIGH_CREDIT', 'TENURE_', 'GENDER', 'MARITAL_STATUS', 'INCOME',
       'CITY_TIER', 'PROFESSION_TYPE', 'TOTAL_WORK_EXPERIENCE',
       'QUALIFICATION', 'EXISTING_EMI', 'EMI_AMOUNT', 'LOAN_PURPOSE', 'CONTRIBUTOR_TYPE_NBF_CCC', 'CONTRIBUTOR_TYPE_COP_OFI_HFC_RRB_MFI', 'CONTRIBUTOR_TYPE_FRB_NAB_PRB_SFB', 'CONTRIBUTOR_TYPE_ARC', 'ACCOUNT_STATUS_ACTIVE', 'ACCOUNT_STATUS_OTHERS']]

print('Initiating Merged function')
df.to_csv('Untitled Folder/Testing Data/Test_{}.csv'.format(confs['PRODUCT_TYPE']), index=False)
print('Final df shape is:', df.shape)
#print('Final columns:', list(merged_data.columns))
print('Successfully created the Testing data')

def Model_Building(df):

    with open('Untitled Folder/Pickle/lr_v0_2.pickle', 'rb') as file:
        # Load the object from the file
        model = pickle.load(file)


    with open('Untitled Folder/Pickle/scaler0_2.pickle', 'rb') as file:
        # Load the object from the file
        scaler = pickle.load(file)

    # Categorical columns for label encoding
    columns = ['GENDER', 'MARITAL_STATUS', 'CITY_TIER', 'PROFESSION_TYPE','TOTAL_WORK_EXPERIENCE','QUALIFICATION',
               'EXISTING_EMI', 'EMI_AMOUNT','LOAN_PURPOSE']

    def label_enq(data, columns):
        from sklearn.preprocessing import LabelEncoder
        label_encoder = LabelEncoder()  
        for column in columns:
            data[column] = label_encoder.fit_transform(data[column])
        return data    

    data = label_enq(df, columns)
    columns_to_scale=['_DISBURSED_AMT_HIGH_CREDIT','TENURE_','INCOME','EMI_AMOUNT']
    df_scaled=pd.DataFrame(scaler.transform(df[columns_to_scale]),columns=columns_to_scale)
    
    data = data.drop(columns = columns_to_scale, axis = 1)
    
    for feat in columns_to_scale:
        data[feat]=df_scaled[feat]

        
    data['ACCOUNT_STATUS_ACTIVE'].fillna(0,inplace=True)
    data['ACCOUNT_STATUS_OTHERS'].fillna(0,inplace=True)
    
    preds=model.predict(data.drop(columns=['CREDT_RPT_ID']))
    tested_df=pd.DataFrame({'CREDT_RPT_ID':data['CREDT_RPT_ID'],'PD':list(preds)})
    tested_df.to_csv('Untitled Folder/Output/Output.csv', index=False)
    
    print('Model Hitted Successfully')
    return

Model_Building(df)


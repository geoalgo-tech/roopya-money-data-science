#Training data
from google.cloud import bigquery
from google.cloud import storage
from pandas_gbq import to_gbq
import pandas as pd
import numpy as np

print('Kartik Puja')
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
    
    #df1 = pd.read_excel(f"{confs['RAW_APPLICATION_DATA']}")
    ##print("Reading Excel File")
    #file_path_w = '{}'.format(confs['APPLICATION_DATA'])
    #df1.to_csv(file_path_w, index = False, index_label = False)

    df2 = pd.read_csv(f"{confs['APPLICATION_DATA']}")

    print('Read successful!!!!!!')
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
    print(df.columns)
    
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

    print(query)
    # Run the query
    query_job = client.query(query)
    results = query_job.result()
    df = results.to_dataframe()
    print('**********', df.shape)

    return df



model_base = model_base_func()
print('Initiating Model Base function')
model_base.to_csv('Untitled Folder/Model_Base_{}.csv'.format(confs['PRODUCT_TYPE']), index=False)
#print('After model_base:', model_base.shape)
print('Initiation done')


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
            WHERE ACCT_TYPE NOT LIKE 'O%t'
AND ACCT_TYPE IN ({sub_types});
            """

    #print(query)
    # Run the query
    query_job = client.query(query)
    results = query_job.result()

    df = results.to_dataframe()

    # df.to_csv('Historical_Data.csv', index=False)
    return df


historical_data = historical_data_func()
print('Initiating Historical function')
historical_data.to_csv('Untitled Folder/Historical_Data_{}.csv'.format(confs['PRODUCT_TYPE']), index=False)
#print('After historical_data:', historical_data.shape)
print('Initiation done')

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
    data1 = pd.read_csv('Untitled Folder/Model_Base_{}.csv'.format(confs['PRODUCT_TYPE']))
    data2 = pd.read_csv('Untitled Folder/Historical_Data_{}.csv'.format(confs['PRODUCT_TYPE']))
    data3 = pd.read_csv('Untitled Folder/IOI_Data_{}.csv'.format(confs['PRODUCT_TYPE']))
    
    def replace(df, column):
        df['CLOSE_DT'] = pd.to_datetime(df['CLOSE_DT'],errors='coerce')
        df['Ref_DISBURSED_DT'] = pd.to_datetime(df['Ref_DISBURSED_DT'])
        mask1 = df['CLOSE_DT'] > df['Ref_DISBURSED_DT']
        mask2 = df['CLOSE_DT'] > '2022-10-31' 
        df.loc[mask1, column] = 'Active'
        df.loc[mask2, column] = 'Active'
        return df

    data2 = replace(data2, 'ACCOUNT_STATUS')
    
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
        # df_t1.rename(columns = {'Good/Bad':'Target'}, inplace = True)
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

########################################################################################################     
    #print('Before Customer_type:', data1.shape)
    data1 = customer_type(data1)
    #print('After Customer_type:', data1.shape)
    
    max_delinquency_df = max_delinquency(data2)
    print('After max_delinquency:', max_delinquency_df.shape)
    print('bvbwde3nbbdnbs:',max_delinquency_df.columns)
    
    print('Before joining max_delinquency:', data1.shape)
    data1 = pd.merge(data1, max_delinquency_df, on='CREDT_RPT_ID', how='left')
    data1.fillna(-1, inplace=True)
    print('After joining max_delinquency:', data1.shape)
    #data1 = pd.merge(data1, customer_type_df, on='CREDT_RPT_ID', how='left')


    print('Before going into Rockys code row number in data2', data2.shape)
    aggregate_df = aggregating_disbursed_and_current_balance(data2)
    print('After coming out from Rocky code:', aggregate_df.shape)
    
    data1 = pd.merge(data1, aggregate_df, on='CREDT_RPT_ID', how='left')
    
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

additional_columns = ['_INCOME_INDICATOR_', '_OCCUPATION', 'CREATED_DATE', 'CUSTOMER_ID', 'TENURE', 'PHONE', 'RESIDENCE_TYPE', 'DESIRED_LOAN_AMOUNT', 'FIRST_DELINQUENCY', 'TOTAL_CURRENT_BAL']

df = df.drop(columns=additional_columns, errors='ignore')

print('Initiating Merged function')
df.to_csv('Untitled Folder/Training Data/{}_Training_Data.csv'.format(confs['PRODUCT_TYPE']), index=False)
print('Final df shape is:', df.shape)
#print('Final columns:', list(merged_data.columns))
print('Successfully created the Training data')
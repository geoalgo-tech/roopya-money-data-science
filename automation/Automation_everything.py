from google.cloud import bigquery
import pandas as pd
import numpy as np


confs = {}
Home_Loan = ['Housing Loan', 'Property Loan', 'Leasing', 'Microfinance Housing Loan']
Credit_Loan = ['Credit Card', 'Loan on Credit Card', 'Secured Credit Card', 'Corporate Credit Card', 'Kisan Credit Card']
Auto_Loan = ['Auto Loan (Personal)', 'Two Wheeler Loan', 'Used Car Loan', 'Commercial Vehicle Loan', 'Used Tractor Loan']
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

    query = """WITH CTE1 AS (
              SELECT app.*,inq.CREDT_RPT_ID 
              FROM `{}` AS inq
              LEFT JOIN `{}` AS app 
              ON inq.PHONE_1 = app.PHONE
            ),

            CTE2 AS (
              SELECT 
                acc.*,
                c.created_date,
                c.customer_id,
                c.phone,
                c.gender AS c_gender,
                c.pan,
                c.marital_status,
                c.income,
                c.city_tier,
                c.state,
                c.profession_type,
                c.residence_type,
                c.total_work_experience,
                c.qualification,
                c.existing_emi,
                c.company_type,
                c.emi_amount,
                c.desired_loan_amount,
                c.tenure,
                c.loan_purpose
              FROM CTE1 AS c 
              LEFT JOIN `{}` AS acc
              ON c.CREDT_RPT_ID = acc.CREDT_RPT_ID
            ),

            CTE3 AS (
              SELECT *,
                ROW_NUMBER() OVER (PARTITION BY CTE2.CREDT_RPT_ID ORDER BY CTE2.DISBURSED_DT DESC) AS rn
              FROM CTE2
              WHERE 
                ACCT_TYPE IN ({})
                AND DATE(CTE2.created_date) < CTE2.DISBURSED_DT
                AND DISBURSED_DT <= DATE '{}' 
                AND DATE_DIFF(DATE_REPORTED, DISBURSED_DT, MONTH) BETWEEN {} AND {}
                AND OWNERSHIP_IND NOT IN ({})
            )

            SELECT * 
            FROM CTE3 
            WHERE rn = 1;""".format(confs['INQUIRY_TABLE'], confs['APPLICATION_TABLE'], confs['ACCOUNT_TABLE'], sub_types, confs['DISBURSED_DATE_END'], confs['DATE_DIFF_START'], confs['DATE_DIFF_END'], loan_subtypes)

    #print(query)
    # Run the query
    query_job = client.query(query)
    results = query_job.result()
    df = results.to_dataframe()

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
                                      
                                      
    query = """WITH CTE1 AS(
                  SELECT * 
                  FROM `{}` as inq
                  LEFT JOIN `{}` as app 
                  ON inq.PHONE_1 = app.PHONE
                ),

                CTE2 AS(
                  SELECT acc.*,c.created_date 
                  FROM CTE1 AS c 
                  LEFT JOIN `{}` as acc
                  ON c.CREDT_RPT_ID = acc.CREDT_RPT_ID
                ),

                CTE3 AS(
                  SELECT *,
                     ROW_NUMBER() OVER (PARTITION BY CTE2.CREDT_RPT_ID ORDER BY CTE2.DISBURSED_DT DESC) AS rn
                     FROM CTE2
                     WHERE ACCT_TYPE IN ({})
                     AND DATE(CTE2.created_date) < CTE2.DISBURSED_DT
                     AND DISBURSED_DT <= DATE '{}' 
                     AND DATE_DIFF(DATE_REPORTED, DISBURSED_DT, MONTH) BETWEEN {} AND {}
                     AND OWNERSHIP_IND NOT IN ({})
                ),

                CTE4 AS(
                  SELECT * 
                  FROM CTE3 c
                  WHERE c.rn = 1
                ),

                CTE5 AS(
                  SELECT T1.* , CTE4.DISBURSED_DT AS Ref_DISBURSED_DT,
                    DATE_DIFF(CTE4.DISBURSED_DT, T1.DISBURSED_DT, MONTH) AS Prev_loan_LOR ,
                    LENGTH(T1.DPD___HIST)/3 AS Count_DPD_stream,
                    DATE_ADD(T1.DATE_REPORTED ,INTERVAL CAST((LENGTH(T1.DPD___HIST)/3*(-1) +1)AS INT64) MONTH) AS DPD_First_month
                    FROM `{}` AS T1 
                    JOIN CTE4 ON T1.CREDT_RPT_ID = CTE4.CREDT_RPT_ID 
                    AND CTE4.DISBURSED_DT > T1.DISBURSED_DT)
                    SELECT * ,DATE_DIFF(Ref_DISBURSED_DT, DPD_First_month, MONTH) AS DPD_month_tb_considered FROM CTE5;""".format(confs['INQUIRY_TABLE'], confs['APPLICATION_TABLE'], confs['ACCOUNT_TABLE'], sub_types, confs['DISBURSED_DATE_END'], confs['DATE_DIFF_START'], confs['DATE_DIFF_END'], loan_subtypes, confs['ACCOUNT_TABLE'])

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


def merged_data_func():
    """
        Reading the Model Base and Historical Data and performing categorization of contributor type, aggregation of account status, customer type, delinquency, disbursed and current balance so that later we can call this one function to get the training data.
        Arguments: NA
        Return: 1 dataframe
    """
    data1 = pd.read_csv('Untitled Folder/Model_Base_{}.csv'.format(confs['PRODUCT_TYPE']))
    data2 = pd.read_csv('Untitled Folder/Historical_Data_{}.csv'.format(confs['PRODUCT_TYPE']))

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

        def extract_max_delinquency_status(bucket):
            if pd.notna(bucket):
                if isinstance(bucket, str):
                    statuses = [int(status) for status in bucket.split(',') if status.isdigit() and int(status) in [0, 1, 2, 3]]
                    if statuses:
                        return max(statuses)
            return 0

        df_t2['max_delinquency_status'] = df_t2['CURRENT_BUCKET'].apply(extract_max_delinquency_status)


        
        df_t2_max_delq = df_t2.groupby(['CREDT_RPT_ID']).agg({
            'max_delinquency_status': 'max'
        }).reset_index()

        

        #print('Max_delinquency function done')
        return df_t2_max_delq
    
    def aggregating_disbursed_and_current_balance(df_q2):
        """
        Aggregation based on Disbursed amount, current balance and return the maximum delinquency of each customer.
        Arguments: 1 dataframe
        Return: 1 dataframe
        """
        agg_df = pd.DataFrame(df_q2['CREDT_RPT_ID'])
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
    #print('After max_delinquency:', max_delinquency_df.shape)
    
    #print('Before joining max_delinquency:', data1.shape)
    data1 = pd.merge(data1, max_delinquency_df, on='CREDT_RPT_ID', how='left')
    #print('After joining max_delinquency:', data1.shape)
    #data1 = pd.merge(data1, customer_type_df, on='CREDT_RPT_ID', how='left')


    #print('Before going into Rockys code', data1.shape)
    aggregate_df = aggregating_disbursed_and_current_balance(data1)
    #print('After coming out from Rocky code:', aggregate_df.shape)
    
    data1 = pd.merge(data1, aggregate_df, on='CREDT_RPT_ID', how='left')
    
    columns_to_drop = [f'M{i}' for i in range(37)]
    additional_columns = ['LOS_APP_ID', 'CANDIDATE___ID', 'CUSTOMER_ID_MBR_ID', 'BRANCH', 'KENDRA', 'SELF_INDICATOR', 'MATCH_TYPE', 'ACC_NUM', 'CREDIT_GRANTOR', 'ACCT_TYPE', 'CONTRIBUTOR_TYPE', 'DATE_REPORTED', 'OWNERSHIP_IND', 'ACCOUNT_STATUS', 'DISBURSED_DT', 'CLOSE_DT', 'LAST_PAYMENT_DATE', 'CREDIT_LIMIT_SANC_AMT', '_INSTALLMENT_AMT', 'INSTALLMENT_FREQUENCY', 'WRITE_OFF_DATE', '_OVERDUE_AMT', '_WRITE_OFF_AMT', 'ASSET_CLASS', '_ACCOUNT_REMARKS', 'LINKED_ACCOUNTS', 'REPORTED_DATE___HIST_', 'DPD___HIST', 'ASSET_CLASS___HIST_', 'HIGH_CRD___HIST_', 'CUR_BAL___HIST_', 'DAS___HIST_', 'AMT_OVERDUE___HIST_', 'AMT_PAID___HIST_', 'Unnamed__41', 'rn']

    columns_to_drop.extend(additional_columns)

    data1 = data1.drop(columns=columns_to_drop, errors='ignore')
    #print('After merging from Rocky code:', data1.shape)
    return data1


######################################################################################################## 

# def snake_case_and_uppercase(column_name):
#     return column_name.replace(' ', '_').upper()

merged_data = merged_data_func()
# merged_data.columns = merged_data.columns.map(snake_case_and_uppercase())
merged_data.columns = merged_data.columns.str.upper()
merged_data.columns = merged_data.columns.str.replace(' ', '_')

print('Initiating Merged function')
merged_data.to_csv('Untitled Folder/Training data_{}.csv'.format(confs['PRODUCT_TYPE']), index=False)
print('Final df shape is:', merged_data.shape)
#print('Final columns:', list(merged_data.columns))
print('Successfully created the Training data')

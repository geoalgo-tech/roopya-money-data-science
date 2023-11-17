from google.cloud import bigquery
import pandas as pd

# Initialize the BigQuery client
client = bigquery.Client()

# Define the SQL query
query = """
    SELECT *
    FROM `geoalgo-208508.roopya_analytics_dw.Kosh_Consolidated_2`
"""
# Run the query
query_job = client.query(query)
results = query_job.result()

# Convert the query results into a DataFrame
df = results.to_dataframe()

# Split the DPD columns into 36 columns
for i in range(36):
    df[f'DAYS_PAST_DUE_HISTORY_MONTH_{i+1}'] = df['DAYS_PAST_DUE_HISTORY'].str[i*3:i*3+3]

# Split Amount Overdue columns
def split_and_convert_column(df, column_name, num_columns):
    
    df_split = df[column_name].str.split(',', expand=True)
    df_split = df_split.apply(pd.to_numeric, errors='coerce')
    df_split.columns = [f'AMOUNT_OVERDUE_HISTORY_MONTH_{i+1}' for i in range(len(df_split.columns))]
    df = pd.concat([df, df_split], axis=1)
    return df

# Use the function to split and convert the column
df_result = split_and_convert_column(df, 'AMOUNT_OVERDUE_HISTORY', 36)

# Divide the dataframe according to applicant type
df_salaried = df[df['KOSH_APPLICANT_TYPE'] == 1]
df_selfemp = df[df['KOSH_APPLICANT_TYPE'] == 2]
df_reject = df[df['KOSH_APPLICANT_TYPE'] == 3]

# Taking only the columns with DPD and amount overdue
dpd_cols = []
for col in df.columns:
    if col.startswith('DAYS_PAST_DUE_HISTORY_MONTH_'):
        dpd_cols.append(col)
    if col.startswith('AMOUNT_OVERDUE_HISTORY_MONTH_'):
        dpd_cols.append(col)

# DPD check for 0, 30+ and 90+         
def low_dpd_check(df):
    df.fillna(0, inplace=True) 
    def check_sum_of_dpd(x):
        dpd_sum = sum(int(dpd) if dpd not in ['XXX', 'DDD', ''] else 0 for dpd in x)
        flag = 0
        if dpd_sum == 0:
            flag = 1
        return flag
    
    low_dpd = df.apply(lambda x: check_sum_of_dpd(x), axis=1)

    return low_dpd

def mid_dpd_check(df):
    df.fillna(0,inplace=True)
    def check_delay_counts(x):
        flag=0
        for dpd in x:
            if dpd in ['XXX','DDD','']:
                dpd=0
            if int(dpd)>90:
                flag=0
                break
            if int(dpd)>30 and int(dpd)<=90:
                flag=1
        return flag
    mid_dpd = df.apply(lambda x:check_delay_counts(x),axis=1)
    print(sum(mid_dpd)) 
    return mid_dpd
    
def high_dpd_check(df):
    df.fillna(0,inplace=True)
    def check_delay_counts(x):
        flag=0
        for dpd in x:
            if dpd in ['XXX','DDD','']:
                dpd=0
            if int(dpd)>90:
                flag=1
                break
        return flag
    high_dpd=df.apply(lambda x:check_delay_counts(x),axis=1)
    return high_dpd           

# All the dataframes with DPD counts        
def extract_dataframes(data, column1, column2):
    dataframes = {}
    values = data[column1].unique()
    for value in values:
        extracted_df = data[data[column1] == value]
        # Low DPD
        low_dpd = low_dpd_check(extracted_df[dpd_cols])
        extracted_df['low_dpd_flag'] = low_dpd
        # Mid DPD
        mid_dpd = mid_dpd_check(extracted_df[dpd_cols])
        extracted_df['mid_dpd_flag'] = mid_dpd
        # High DPD
        high_dpd = high_dpd_check(extracted_df[dpd_cols])
        extracted_df['high_dpd_flag'] = high_dpd
        
        def assign_dpd_list(row):
            if row['high_dpd_flag'] == 1:return 4
            elif row['mid_dpd_flag'] == 1 and row['high_dpd_flag'] == 0 :return 3
            elif row['low_dpd_flag'] == 1 and row['mid_dpd_flag'] == 0 and row['high_dpd_flag'] == 0:return 2
            else:return 1
 
        extracted_df = extracted_df[['CREDIT_REPORT_ID', 'CONTRIBUTOR_TYPE', 'low_dpd_flag', 'mid_dpd_flag', 'high_dpd_flag']]
        extracted_df['DPD_List'] = extracted_df.apply(assign_dpd_list, axis=1)
        # Pivoting DF
        e_df = extracted_df.pivot_table(index = 'CREDIT_REPORT_ID', columns = 'DPD_List', aggfunc = 'size', fill_value = 0)
        e_df.reset_index(inplace=True)
        # Rename columns for better readability
        e_df.rename(columns={1: 'DPD_1_to_30', 2: 'DPD_0', 3: 'DPD_30_to_90', 4: 'DPD_More_than_90'}, inplace=True)
        def assign_dpd_list(row):
            if row['DPD_More_than_90'] > 0: return 4
            elif row['DPD_30_to_90'] > 0  and row['DPD_More_than_90'] == 0 : return 3
            elif row['DPD_1_to_30'] > 0 and row['DPD_30_to_90'] == 0 and row['DPD_More_than_90'] == 0:return 1
            else: return 2

        e_df['Final_DPD_List'] = e_df.apply(assign_dpd_list, axis=1)
        ref_df = extracted_df[['CREDIT_REPORT_ID', 'CONTRIBUTOR_TYPE']]
        
        df_1 = e_df.merge(ref_df,on='CREDIT_REPORT_ID',how='left')
        print(df_1.shape)
        df_2 = df_1.drop_duplicates()
        print(df_2.shape)
        values = df_2[column2].unique()
        for value in values: 
            extracted_df_ct = df_2[df_2[column2] == value]
            dataframes[value] = extracted_df_ct
            print('CONTRIBUTOR_TYPE:', value)
            print(extracted_df_ct['Final_DPD_List'].value_counts())
            
extract_dataframes(df, 'KOSH_APPLICANT_TYPE', 'CONTRIBUTOR_TYPE')

# CONTRIBUTOR_TYPE is the variable column we can use any columns for doing this DPD analysis for high mid and low DPD (90+ and 30-90 and 0)
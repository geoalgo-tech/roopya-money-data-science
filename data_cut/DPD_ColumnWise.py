from google.cloud import bigquery
import pandas as pd

# Initialize the BigQuery client
client = bigquery.Client()

# Define the SQL query
query = """
    SELECT *
    FROM `geoalgo-208508.roopya_analytics_dw.Kosh_Consolidated_4`
"""
# Run the query
query_job = client.query(query)
results = query_job.result()

# Convert the query results into a DataFrame
df = results.to_dataframe()

df_salaried = df[df['KOSH_APPLICANT_TYPE'] == 1]
df_selfemp = df[df['KOSH_APPLICANT_TYPE'] == 2]
df_reject = df[df['KOSH_APPLICANT_TYPE'] == 3]

dpd_cols = []
for col in df.columns:
    if col.startswith('DAYS_PAST_DUE_HISTORY_MONTH_'):
        dpd_cols.append(col)
    if col.startswith('AMOUNT_OVERDUE_HISTORY_MONTH_'):
        dpd_cols.append(col)
        
import pandas as pd

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

# CONTRIBUTOR_TYPE is the variable column we can use any columns for doing this DPD analysis for hig
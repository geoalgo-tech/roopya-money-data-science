from google.cloud import bigquery
import pandas as pd

# Initialize the BigQuery client
client = bigquery.Client()

# Define the SQL query
query = """
    SELECT *
    FROM `geoalgo-208508.roopya_analytics_dw.CF_Mixed`
"""
# Run the query
query_job = client.query(query)
results = query_job.result()

# Convert the query results into a DataFrame
df = results.to_dataframe()

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

# Taking the DPD hist and AMT OverDue hist columns and keep that in different dataframe
dpd_col = [f'DAYS_PAST_DUE_HISTORY_MONTH_{i}' for i in range(1, 37)]
dpd_df = df[dpd_col].copy()
amt_col = [f'AMOUNT_OVERDUE_HISTORY_MONTH_{i}' for i in range(1, 37)]
amt_df = df[amt_col].copy()

# Concatinating both the Dataframe to a single dataframe
m_df = pd.concat([dpd_df, amt_df], axis = 1)

# Initialize a column for customer categorization on account level
m_df['ROOPYA_ACCOUNT_STATUS'] = 'Uncategorised'

# Condition 1: Last 36 months all DPD is 0
condition_1 = m_df.iloc[:, 1:37].isin([0]).all(axis=1)  
m_df.loc[condition_1, 'ROOPYA_ACCOUNT_STATUS'] = 'Good'

# Condition 2: Last 6 months DPD 0 and rest DPD amount < 5000
condition_2 = ((m_df.iloc[:, 1:7].isin([0]).all(axis=1)) &
                (m_df.iloc[:, 43:73].applymap(lambda x: float(x) if str(x).isdigit() else 0).sum(axis=1) <5000))
m_df.loc[condition_2, 'ROOPYA_ACCOUNT_STATUS'] = 'Good'

# Condition 3: Last 6 months any DPD is more than 0
condition_3 = (~m_df.iloc[:, 1:7].isin([0]).all(axis=1))
m_df.loc[condition_3, 'ROOPYA_ACCOUNT_STATUS'] = 'Bad'

# Condition 4: Last 36 months any DPD > 5000
condition_4 = (m_df.iloc[:, 37:73].applymap(lambda x: float(x) if str(x).isdigit() else 0) > 5000).any(axis=1)
m_df.loc[condition_4, 'ROOPYA_ACCOUNT_STATUS'] = 'Bad'

# Keeping the ROOPYA_ACCOUNT_STATUS and keep that in another dataframe
cs_col = ['ROOPYA_ACCOUNT_STATUS']
cs_df = m_df[cs_col].copy()

# Concatinating the main df and the created datafrme with ROOPYA_ACCOUNT_STATUS
final_df = pd.concat([df, cs_df], axis = 1)

# Nullify the dataframes which shouldn't be used further
dpd_df = None
amt_df = None
cs_df = None
m_df = None 

# List of columns to remove
columns_to_remove = ['SELF_INDICATOR', 'MATCH_TYPE', 'DISBURSED_DATE', 'DATE_REPORTED', 'DAYS_PAST_DUE_HISTORY_MONTH_1', 'DAYS_PAST_DUE_HISTORY_MONTH_2','DAYS_PAST_DUE_HISTORY_MONTH_3', 'DAYS_PAST_DUE_HISTORY_MONTH_4','DAYS_PAST_DUE_HISTORY_MONTH_5', 'DAYS_PAST_DUE_HISTORY_MONTH_6','DAYS_PAST_DUE_HISTORY_MONTH_7', 'DAYS_PAST_DUE_HISTORY_MONTH_8','DAYS_PAST_DUE_HISTORY_MONTH_9', 'DAYS_PAST_DUE_HISTORY_MONTH_10','DAYS_PAST_DUE_HISTORY_MONTH_11', 'DAYS_PAST_DUE_HISTORY_MONTH_12','DAYS_PAST_DUE_HISTORY_MONTH_13', 'DAYS_PAST_DUE_HISTORY_MONTH_14','DAYS_PAST_DUE_HISTORY_MONTH_15', 'DAYS_PAST_DUE_HISTORY_MONTH_16','DAYS_PAST_DUE_HISTORY_MONTH_17', 'DAYS_PAST_DUE_HISTORY_MONTH_18','DAYS_PAST_DUE_HISTORY_MONTH_19', 'DAYS_PAST_DUE_HISTORY_MONTH_20', 'DAYS_PAST_DUE_HISTORY_MONTH_21', 'DAYS_PAST_DUE_HISTORY_MONTH_22','DAYS_PAST_DUE_HISTORY_MONTH_23', 'DAYS_PAST_DUE_HISTORY_MONTH_24', 'DAYS_PAST_DUE_HISTORY_MONTH_25', 'DAYS_PAST_DUE_HISTORY_MONTH_26','DAYS_PAST_DUE_HISTORY_MONTH_27', 'DAYS_PAST_DUE_HISTORY_MONTH_28','DAYS_PAST_DUE_HISTORY_MONTH_29', 'DAYS_PAST_DUE_HISTORY_MONTH_30','DAYS_PAST_DUE_HISTORY_MONTH_31', 'DAYS_PAST_DUE_HISTORY_MONTH_32','DAYS_PAST_DUE_HISTORY_MONTH_33', 'DAYS_PAST_DUE_HISTORY_MONTH_34','DAYS_PAST_DUE_HISTORY_MONTH_35', 'DAYS_PAST_DUE_HISTORY_MONTH_36','AMOUNT_OVERDUE_HISTORY_MONTH_1', 'AMOUNT_OVERDUE_HISTORY_MONTH_2','AMOUNT_OVERDUE_HISTORY_MONTH_3', 'AMOUNT_OVERDUE_HISTORY_MONTH_4','AMOUNT_OVERDUE_HISTORY_MONTH_5', 'AMOUNT_OVERDUE_HISTORY_MONTH_6','AMOUNT_OVERDUE_HISTORY_MONTH_7', 'AMOUNT_OVERDUE_HISTORY_MONTH_8','AMOUNT_OVERDUE_HISTORY_MONTH_9', 'AMOUNT_OVERDUE_HISTORY_MONTH_10','AMOUNT_OVERDUE_HISTORY_MONTH_11', 'AMOUNT_OVERDUE_HISTORY_MONTH_12','AMOUNT_OVERDUE_HISTORY_MONTH_13', 'AMOUNT_OVERDUE_HISTORY_MONTH_14','AMOUNT_OVERDUE_HISTORY_MONTH_15', 'AMOUNT_OVERDUE_HISTORY_MONTH_16','AMOUNT_OVERDUE_HISTORY_MONTH_17', 'AMOUNT_OVERDUE_HISTORY_MONTH_18','AMOUNT_OVERDUE_HISTORY_MONTH_19', 'AMOUNT_OVERDUE_HISTORY_MONTH_20','AMOUNT_OVERDUE_HISTORY_MONTH_21', 'AMOUNT_OVERDUE_HISTORY_MONTH_22','AMOUNT_OVERDUE_HISTORY_MONTH_23', 'AMOUNT_OVERDUE_HISTORY_MONTH_24','AMOUNT_OVERDUE_HISTORY_MONTH_25', 'AMOUNT_OVERDUE_HISTORY_MONTH_26','AMOUNT_OVERDUE_HISTORY_MONTH_27', 'AMOUNT_OVERDUE_HISTORY_MONTH_28','AMOUNT_OVERDUE_HISTORY_MONTH_29', 'AMOUNT_OVERDUE_HISTORY_MONTH_30','AMOUNT_OVERDUE_HISTORY_MONTH_31', 'AMOUNT_OVERDUE_HISTORY_MONTH_32','AMOUNT_OVERDUE_HISTORY_MONTH_33', 'AMOUNT_OVERDUE_HISTORY_MONTH_34','AMOUNT_OVERDUE_HISTORY_MONTH_35', 'AMOUNT_OVERDUE_HISTORY_MONTH_36', 'AMOUNT']

# Remove the specified columns
final_df_cleaned = final_df.drop(columns=columns_to_remove)

# Saving the dataframe for further use
final_df = final_df_cleaned

# List of columns to replace
columns_to_process = ['CURRENT_BALANCE', 'INCOME', 'OVERDUE_AMOUNT']

# Replace null by 0 
final_df[columns_to_process] = final_df[columns_to_process].fillna(0)

# Filtering the dataframe to remove extreme values
final_df1 = final_df[(final_df['CURRENT_BALANCE'] >= 0) & (final_df['CURRENT_BALANCE'] <= 10000000) & (final_df['PRIMARY_NO_OF_ACCOUNTS'] <= 100)]

# Saving the dataframe for further use
df = final_df1

# Again filtering the dataframe to remove extreme values
df1 = df[(df['PRIMARY_CURRENT_BALANCE'] >= 0) & (df['PRIMARY_CURRENT_BALANCE'] <= 10000000) & (df['PRIMARY_SANCTIONED_AMOUNT'] >= 0) & (df['PRIMARY_SANCTIONED_AMOUNT'] <= 10000000) & (df['PRIMARY_DISTRIBUTED_AMOUNT'] >= 0) & (df['PRIMARY_DISTRIBUTED_AMOUNT'] <= 10000000) & (df['PRIMARY_INSTALLMENT_AMOUNT'] >= 0) & (df['PRIMARY_INSTALLMENT_AMOUNT'] <= 10000000) & (df['INCOME'] >= 0) & (df['INCOME'] <= 10000000)]

# Copying the dataframe for further use
df = df1.copy()

# Specify the columns to consider for duplicates
columns_to_check = ['CREDIT_REPORT_ID','PRIMARY_NO_OF_ACCOUNTS', 'PRIMARY_ACTIVE_ACCOUNTS', 'PRIMARY_OVERDUE_ACCOUNTS', 'PRIMARY_CURRENT_BALANCE', 'PRIMARY_SANCTIONED_AMOUNT', 'PRIMARY_DISTRIBUTED_AMOUNT', 'PRIMARY_INSTALLMENT_AMOUNT', 'NEW_ACCOUNTS_IN_LAST_SIX_MONTHS', 'AVERAGE_ACCOUNT_AGE', 'NO_OF_INQUIRIES', 'INCOME', 'AGE_COHORT', 'STATE']

# Drop duplicates based on the specified columns
cl_df = df.drop_duplicates(subset='CREDIT_REPORT_ID')

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

# Merging with the original dataframe
merged_df = acc_df.merge(df, on="CREDIT_REPORT_ID", how="left")

# Duplication dropping
acty_df = merged_df.drop_duplicates(subset='CREDIT_REPORT_ID')

# Pivot CONTRIBUTOR_TYPE data
ct_df = df.pivot_table(index = 'CREDIT_REPORT_ID', columns = 'CONTRIBUTOR_TYPE', aggfunc ='size', fill_value = 0)

# Reset index to have 'CREDIT_REPORT_ID' as a regular column
ct_df.reset_index(inplace=True)

# Rename columns for better readability
ct_df.rename(columns={'PRB': 'CONTRIBUTOR_TYPE_PRB', 'NBF': 'CONTRIBUTOR_TYPE_NBF'}, inplace=True)

# Merging the previous dataframe with pivoted CONTRIBUTOR_TYPE dataframe
merged_df = acty_df.merge(ct_df, on="CREDIT_REPORT_ID", how="left")
        
# Pivot OWNERSHIP_IND data
oi_df = df.pivot_table(index = 'CREDIT_REPORT_ID', columns = 'OWNERSHIP_IND', aggfunc = 'size', fill_value = 0)

# Reset index to have 'CREDIT_REPORT_ID' as a regular column
oi_df.reset_index(inplace=True)

# Rename columns for better readability
oi_df.rename(columns={'Individual': 'OWNERSHIP_IND_Individual', 'Supl Card Holder': 'OWNERSHIP_IND_Supl Card Holder', 'Joint': 'OWNERSHIP_IND_Joint', 'Guarantor': 'OWNERSHIP_IND_Guarantor'}, inplace=True)

# Merging the previous dataframe with pivoted OWNERSHIP_IND dataframe
merged1_df = merged_df.merge(oi_df, on="CREDIT_REPORT_ID", how="left")

# Pivot ACCOUNT_STATUS data
as_df = df.pivot_table(index='CREDIT_REPORT_ID', columns='ACCOUNT_STATUS', aggfunc='size', fill_value=0)

# Reset index and remove the name of the columns index
as_df.reset_index(inplace=True)

# Rename columns for better readability
as_df.rename(columns={'Active': 'ACCOUNT_STATUS_Active', 'Closed': 'ACCOUNT_STATUS_Closed'}, inplace=True)

# Merging the previous dataframe with pivoted ACCOUNT_STATUS dataframe
merged2_df = merged1_df.merge(as_df, on="CREDIT_REPORT_ID", how="left")

# Taking the below dataframe for pivoting ACCOUNT_STATUS aggrigation with CURRENT_BALANCE and OVERDUE_AMOUNT
agg_data = df[['CREDIT_REPORT_ID', 'ACCOUNT_STATUS', 'CURRENT_BALANCE', 'OVERDUE_AMOUNT']]

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
    return pivot_df

# Call the function to aggregate balances
aggregated_data = aggregation(agg_data)

# Taking the previous pivoted dataframe and merging with merged dataframe
merged3_df = merged2_df.merge(aggregated_data, on="CREDIT_REPORT_ID", how="left")

# Getting the unique good and bad customer from account level to customer level
cri_df = merged3_df[['CREDIT_REPORT_ID', 'ROOPYA_ACCOUNT_STATUS']]

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

# Dropping the unnecessary columns
df_1 = merged3_df.drop(columns = ['INCOME_BAND', 'ACCOUNT_TYPE', 'CONTRIBUTOR_TYPE', 'OWNERSHIP_IND', 'ACCOUNT_STATUS','ROOPYA_ACCOUNT_STATUS', 'Loan Category', 'Primary'])
df_2 = final_status_df.drop(columns = ['CREDIT_REPORT_ID'])

# Reseting index for concatinating
df1 = df_1.reset_index(drop=True)
df2 = df_2.reset_index(drop=True)

# Concatinating both the dataframes
result_df = pd.concat([df1, df2], axis = 1)

# Changing the column name
final_df = result_df.rename(columns={'Home Loan': 'ACCOUNT_TYPE_Home Loan'})

# Flitering the dataframe for removing extreme outlier values 
df10 = final_df[(final_df['Active_OVERDUE_AMOUNT'] >= 0)]

# Dropping the unnecessary columns
df10 = df10.drop(columns = ['Closed_CURRENT_BALANCE', 'Closed_OVERDUE_AMOUNT'], axis = 1)


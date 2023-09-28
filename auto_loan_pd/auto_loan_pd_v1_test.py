import pandas as pd
import numpy as np


###
from google.cloud import bigquery

# Initialize the BigQuery client
client = bigquery.Client()

# Define the SQL query
query = """
    SELECT *
    FROM `geoalgo-208508.roopya_analytics_dw.Consolidated_Customer_test`
"""
# Run the query
query_job = client.query(query)
results = query_job.result()

# Convert the query results into a DataFrame
df = results.to_dataframe()



df = df[df['ACCOUNT_TYPE_Auto_Loan'] != 0]
df = df.reset_index(drop=True)


# Function to convert years and months to months
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


columns_to_standardize = [
    'ACCOUNT_TYPE_Auto_Loan',
    'ACCOUNT_TYPE_Credit_Card',
    'ACCOUNT_TYPE_Home_Loan',
    'ACCOUNT_TYPE_Personal_Loan',
    'PRIMARY_NO_OF_ACCOUNTS',
    'PRIMARY_ACTIVE_ACCOUNTS',
    'PRIMARY_OVERDUE_ACCOUNTS',
    'PRIMARY_CURRENT_BALANCE',
    'PRIMARY_SANCTIONED_AMOUNT',
    'PRIMARY_DISTRIBUTED_AMOUNT',
    'PRIMARY_INSTALLMENT_AMOUNT',
    'NEW_ACCOUNTS_IN_LAST_SIX_MONTHS',
    'AVERAGE_ACCOUNT_AGE',
    'NO_OF_INQUIRIES',
    'INCOME',
    'CONTRIBUTOR_TYPE_NBF', 'CONTRIBUTOR_TYPE_PRB',
    'OWNERSHIP_IND_Guarantor',
    'OWNERSHIP_IND_Individual',
    'OWNERSHIP_IND_Joint',
    'OWNERSHIP_IND_Supl_Card_Holder',
    'ACCOUNT_STATUS_Active',
    'ACCOUNT_STATUS_Closed',
    'Active_CURRENT_BALANCE',
    'Active_OVERDUE_AMOUNT',
]


import pandas as pd
from sklearn.preprocessing import StandardScaler

# Initialize the StandardScaler
scaler = StandardScaler()


# Fit and transform the numerical columns using the scaler
df[columns_to_standardize] = scaler.fit_transform(df[columns_to_standardize])

# Create a new DataFrame with the scaled data
scaled_df = df.copy()


df1 = scaled_df.copy()


df1 = df1.drop(columns = ['PRIMARY_SANCTIONED_AMOUNT'])

##############################
# import pandas as pd
# import numpy as np
# import json

# def woe_discrete(df, cat_variable_name, y_df):
#     # Create a new DataFrame combining the categorical variable and the target variable
#     df = pd.concat([df[cat_variable_name], y_df], axis=1)
    
#     # Calculate the count and mean of the target variable for each category
#     df = pd.concat([df.groupby(df.columns.values[0], as_index=False)[df.columns.values[1]].count(),
#                     df.groupby(df.columns.values[0], as_index=False)[df.columns.values[1]].mean()], axis=1)
    
#     # Select the relevant columns
#     df = df.iloc[:, [0, 1, 3]]
#     df.columns = [df.columns.values[0], 'n_obs', 'prop_good']
    
#     # Calculate proportions
#     df['prop_n_obs'] = df['n_obs'] / df['n_obs'].sum()
#     df['n_good'] = df['prop_good'] * df['n_obs']
#     df['n_bad'] = (1 - df['prop_good']) * df['n_obs']
#     df['prop_n_good'] = df['n_good'] / (df['n_good'].sum() + 1e-10)  # Avoid division by zero
#     df['prop_n_bad'] = df['n_bad'] / (df['n_bad'].sum() + 1e-10)  # Avoid division by zero
    
#     # Calculate WoE
#     df['WoE'] = np.log(df['prop_n_good'] / df['prop_n_bad'])
    
#     # Replace infinite values with the median WoE
#     median_woe = np.median(df['WoE'][np.isfinite(df['WoE'])])
#     df['WoE'].replace([np.inf, -np.inf], median_woe, inplace=True)
    
#     df = df.sort_values(['WoE'])
#     df = df.reset_index(drop=True)
    
#     return df

# # Assuming 'ROOPYA_CUSTOMER_STATUS' is a column in your DataFrame
# y_df = df2['ROOPYA_CUSTOMER_STATUS']

# # Calculate WoE for 'STATE' column
# woe_state_df = woe_discrete(df2, 'STATE', y_df)

# # Calculate WoE for 'AGE_COHORT' column
# woe_age_cohort_df = woe_discrete(df2, 'AGE_COHORT', y_df)

# # Create dictionaries mapping states and age cohorts to their WoE values
# state_woe_dict = dict(zip(woe_state_df['STATE'], woe_state_df['WoE']))
# age_cohort_woe_dict = dict(zip(woe_age_cohort_df['AGE_COHORT'], woe_age_cohort_df['WoE']))

# # Save the dictionaries to separate JSON files
# with open('state_woe_values.json', 'w') as json_file:
#     json.dump(state_woe_dict, json_file, indent=4)  # Indent for better readability

# with open('age_cohort_woe_values.json', 'w') as json_file:
#     json.dump(age_cohort_woe_dict, json_file, indent=4)  # Indent for better readability

# print("WoE values saved to 'state_woe_values.json' and 'age_cohort_woe_values.json'.")

################

# # Map WoE values to 'STATE' column in the test dataset
test_data['STATE_WOE'] = test_data['STATE'].map(state_woe_dict)

# # Map WoE values to 'AGE_COHORT' column in the test dataset
test_data['AGE_COHORT_WOE'] = test_data['AGE_COHORT'].map(age_cohort_woe_dict)

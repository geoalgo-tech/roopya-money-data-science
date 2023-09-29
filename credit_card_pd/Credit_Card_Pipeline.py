pip install imbalanced-learn

# Import necessary libraries
from google.cloud import bigquery
import re
import pandas as pd
import numpy as np
from scipy.stats.mstats import winsorize
from sklearn.preprocessing import StandardScaler
import seaborn as sns
import matplotlib.pyplot as plt
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
from imblearn.over_sampling import SMOTE
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import classification_report, accuracy_score, precision_score, recall_score, f1_score, roc_curve, precision_recall_curve, auc, confusion_matrix
import joblib

# Initialize the BigQuery client
client = bigquery.Client()

# Define the SQL query
query = """
    SELECT *
    FROM `geoalgo-208508.roopya_analytics_dw.Consolidated_Customer`
"""
# Run the query
query_job = client.query(query)
results = query_job.result()

# Convert the query results into a DataFrame
df = results.to_dataframe()

# Function to convert year month (str) to total months (int)
def convert_to_months(duration_str):
    pattern = r'(\d+)\s*yrs\s+(\d+)\s*mon'
    match = re.match(pattern, duration_str, re.IGNORECASE)
    if match:
        years = int(match.group(1))
        months = int(match.group(2# Initialize the BigQuery client
        total_months = years * 12 + months
        return total_months
    else:
        return None

# Apply the function to the AVERAGE_ACCOUNT_AGE column
df['AVERAGE_ACCOUNT_AGE_IN_MONTHS'] = df['AVERAGE_ACCOUNT_AGE'].apply(lambda x: convert_to_months(x))
                                 
# Keep the rows where a customer has atleast one Credit Card
df = df[df['ACCOUNT_TYPE_Credit_Card'] != 0]

# Resetting index after some rows are deleted
df = df.reset_index(drop=True)
                                 
# Replace the str value Good by 1 and Bad by 0 for final modelling
df['ROOPYA_CUSTOMER_STATUS'] = df['ROOPYA_CUSTOMER_STATUS'].replace({'Bad': 0, 'Good': 1})
                                 
# Null values are present in the age cohort that need to be replaced

# Calculate the median of the column
median_value = df['AGE_COHORT'].median()

# Replace null values with the median value
df['AGE_COHORT'].fillna(median_value, inplace=True)
                                 
                                
# Null values are present in the age cohort that need to be replaced

# Calculate the median of the column
median_value = df['AGE_COHORT'].median()

# Replace null values with the median value
df['AGE_COHORT'].fillna(median_value, inplace=True)
                                 
# Columns to scalling and replacing outliers
columns_to_deal = ['ACCOUNT_TYPE_Auto_Loan', 'ACCOUNT_TYPE_Credit_Card', 'ACCOUNT_TYPE_Home_Loan', 'ACCOUNT_TYPE_Personal_Loan', 'PRIMARY_NO_OF_ACCOUNTS', 'PRIMARY_ACTIVE_ACCOUNTS', 'PRIMARY_OVERDUE_ACCOUNTS', 'PRIMARY_CURRENT_BALANCE', 'PRIMARY_SANCTIONED_AMOUNT', 'PRIMARY_DISTRIBUTED_AMOUNT', 'PRIMARY_INSTALLMENT_AMOUNT', 'NEW_ACCOUNTS_IN_LAST_SIX_MONTHS', 'NO_OF_INQUIRIES', 'Active_CURRENT_BALANCE', 'Active_OVERDUE_AMOUNT',  'INCOME', 'AGE_COHORT', 'CONTRIBUTOR_TYPE_NBF', 'CONTRIBUTOR_TYPE_PRB', 'OWNERSHIP_IND_Guarantor', 'OWNERSHIP_IND_Individual', 'OWNERSHIP_IND_Joint', 'OWNERSHIP_IND_Supl_Card_Holder', 'ACCOUNT_STATUS_Active', 'ACCOUNT_STATUS_Closed', 'AVERAGE_ACCOUNT_AGE_IN_MONTHS']
                                 
# Remove the unique credit report id, target variable and unnecessary column
df1 = df.drop(['CREDIT_REPORT_ID', 'ROOPYA_CUSTOMER_STATUS', 'AVERAGE_ACCOUNT_AGE', 'STATE', 'CURRENT_BALANCE', 'OVERDUE_AMOUNT'], axis = 1)
cri_df = df['CREDIT_REPORT_ID']
                                 
# Calculating the quartiles
Q1 = df1[columns_to_deal].quantile(0.25)
Q3 = df1[columns_to_deal].quantile(0.75)
IQR = Q3 - Q1

# Define the lower and upper bounds for outliers
lower_bound = Q1 - 1.5 * IQR
upper_bound = Q3 + 1.5 * IQR

# Outlier columns
outliers = ['ACCOUNT_TYPE_Credit_Card', 'ACCOUNT_TYPE_Personal_Loan', 'PRIMARY_NO_OF_ACCOUNTS', 'PRIMARY_ACTIVE_ACCOUNTS', 'PRIMARY_CURRENT_BALANCE', 'PRIMARY_SANCTIONED_AMOUNT', 'PRIMARY_DISTRIBUTED_AMOUNT', 'PRIMARY_INSTALLMENT_AMOUNT', 'NEW_ACCOUNTS_IN_LAST_SIX_MONTHS', 'Active_CURRENT_BALANCE', 'AGE_COHORT', 'CONTRIBUTOR_TYPE_NBF', 'CONTRIBUTOR_TYPE_PRB', 'OWNERSHIP_IND_Individual', 'ACCOUNT_STATUS_Active', 'ACCOUNT_STATUS_Closed', 'AVERAGE_ACCOUNT_AGE_IN_MONTHS']
                                 
# Replacing outliers with median
outlier_deal = df1[outliers]

def replace_outliers_with_median(df, threshold=1.5):

    # Filter numeric columns in the DataFrame
    numeric_columns = df.select_dtypes(include=[np.number])

    # Calculate the median for each numeric column
    median = numeric_columns.median()

    # Calculate the interquartile range (IQR) for each column
    q1 = numeric_columns.quantile(0.25)
    q3 = numeric_columns.quantile(0.75)
    iqr = q3 - q1

    # Calculate lower and upper bounds for outliers for each column
    lower_bound = q1 - threshold * iqr
    upper_bound = q3 + threshold * iqr

    # Replace outliers in each numeric column with the median
    for column in numeric_columns.columns:
        df[column] = np.where(
            (df[column] < lower_bound[column]) | (df[column] > upper_bound[column]),
            median[column],
            df[column]
        )

    return df

result_df = replace_outliers_with_median(outlier_deal)
                                 
# Remove columns with outlier from the data 
df_1 = df1.drop(columns = outliers, axis = 1)
df_3 = df['ROOPYA_CUSTOMER_STATUS']

# Reseting index for concatinating 
df_1 = df_1.reset_index()
df_2 = result_df.reset_index()
df_3 = df_3.reset_index()

#Concatination
sg_df1 = pd.concat([df_1, df_2], axis = 1)
sg_df = pd.concat([sg_df1, df_3], axis = 1)
sg_df = sg_df.drop(columns = 'index', axis = 1)
                                 
# Calculate correlation matrix
correlation_matrix = sg_df.corr()

plt.figure(figsize=(25, 18))
sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm', center=0)
plt.title('Correlation Matrix')
plt.show()
                                 
# Partition of data for model training
X = sg_df.drop(columns = ['ROOPYA_CUSTOMER_STATUS', 'PRIMARY_SANCTIONED_AMOUNT'], axis = 1)
y = sg_df['ROOPYA_CUSTOMER_STATUS']
                                 
# Train test split
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Apply SMOTE to balance the classes
smote = SMOTE(sampling_strategy='auto', random_state=42)
X_train = X_train.astype('float64')
y_train = y_train.astype('float64')
X_train_resampled, y_train_resampled = smote.fit_resample(X_train, y_train)
                                 
# Create a pipeline with StandardScaler, and LogisticRegression
pipeline = Pipeline([
    ('scaler', StandardScaler()),               # Step 1: StandardScaler for feature scaling
    ('classifier', LogisticRegression(penalty='l2', C=1.0, solver='lbfgs', max_iter=1000, random_state=42))  # Step 2: Logistic Regression
])
                                 
# Fit the pipeline to your data
pipeline.fit(X_train_resampled, y_train_resampled)
                                 
# Pipeline can be used for predictions
y_pred = pipeline.predict(X_test)
                                 
# Calculate accuracy
accuracy = accuracy_score(y_test, y_pred)
print(f"LR_Accuracy: {accuracy: }")

# Calculate precision
precision = precision_score(y_test, y_pred)
print(f"LR_Precision: {precision: }")

# Calculate recall
recall = recall_score(y_test, y_pred)
print(f"LR_Recall: {recall: }")

# Calculate f1_score
lr_f1 = f1_score(y_test, y_pred)
print(f"LR_F1_Score: {lr_f1: }")

# Display classification report
print("LR Classification Report:")
print(classification_report(y_test, y_pred))
                                 
# Define the filename for saving the XGBoost classifier model
pipeline_filename = 'CC_LR_pipeline_model.joblib'

# Save the trained model to a file
joblib.dump(pipeline, pipeline_filename)

print(f"Credit Card LR pipeline model saved as '{pipeline_filename}'")
                                 
# Initialize the BigQuery client
client = bigquery.Client()

# Define the SQL query
query = """
    SELECT *
    FROM `geoalgo-208508.roopya_analytics_dw.CC_Pipeline_Batch_Prediction`
"""
# Run the query
query_job = client.query(query)
results = query_job.result()

# Convert the query results into a DataFrame
test_df = results.to_dataframe()
                                 
# Initialize the BigQuery client
client = bigquery.Client()

# Define the SQL query
query = """
    SELECT *
    FROM `geoalgo-208508.roopya_analytics_dw.Credit_Card_Testing`
"""
# Run the query
query_job = client.query(query)
results = query_job.result()

# Convert the query results into a DataFrame
test = results.to_dataframe()

# Find the datafrmes for test                                 
y_test = test['ROOPYA_CUSTOMER_STATUS'] 
y_test = y_test.astype('int')
test_df['prediction'] = test_df['prediction'].apply(lambda x: round(float(x))).astype(int)
y_pred = test_df['prediction'] 
                                 
# Calculate accuracy
accuracy = accuracy_score(y_test, y_pred)
print(f"LR_Accuracy: {accuracy: }")

# Calculate precision
precision = precision_score(y_test, y_pred)
print(f"LR_Precision: {precision: }")

# Calculate recall
recall = recall_score(y_test, y_pred)
print(f"LR_Recall: {recall: }")

# Calculate f1_score
lr_f1 = f1_score(y_test, y_pred)
print(f"LR_F1_Score: {lr_f1: }")

# Display classification report
print("LR Classification Report:")
print(classification_report(y_test, y_pred))                                 
                                 
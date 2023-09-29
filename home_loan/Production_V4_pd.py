dh = df[df['ACCOUNT_TYPE_Home_Loan'] != 0]     # for home loan 


#------------------------------------------------------------------------------Handling Null Values-------------------------------------
from sklearn.impute import SimpleImputer
# handling with mean 
dh['AGE_COHORT'].fillna(dh['AGE_COHORT'].mean(), inplace=True)


#--------------------------------------------------------------------------------FEATURE ENGENNEEING----------------------------------------------------------
# Convert AVERAGE_ACCOUNT_AGE to the total number of months
def convert_to_months(age_str):
    years, months = age_str.split(' ')
    total_months = int(years.strip('yrs')) * 12 + int(months.strip('mon'))
    return total_months

dh['AVERAGE_ACCOUNT_AGE'] = dh['AVERAGE_ACCOUNT_AGE'].apply(convert_to_months)


#----------------------------------------------------------------------------------------CONVERt into Binary Vlues, Good:1, Bad:0--------------------------------
# Assuming 'Roopya_customer_Status' is a column in your DataFrame
dh['ROOPYA_CUSTOMER_STATUS'] = dh['ROOPYA_CUSTOMER_STATUS'].map({'Good': 1, 'Bad': 0})


#------------------------------------------------------------------------------------Planning With WOE, weight of evidence--------------------
# Define a function to calculate WOE and IV
#______________________________________________________________________________ ___________FUNTION-START
def calculate_woe_iv(df, categorical_column, target_column):
    # Calculate the total count of events and non-events
    
    total_events = df[target_column].sum()
    total_non_events = df.shape[0] - total_events
    
    woe_dict = {}
    iv = 0
    
    # Loop through unique categories in the categorical column
    for category in df[categorical_column].unique():
        category_df = df[df[categorical_column] == category]
        events_in_category = category_df[target_column].sum()
        non_events_in_category = category_df.shape[0] - events_in_category
        
        # Calculate percentage of events and non-events in the category
        percentage_events = (events_in_category + 1) / (total_events + 1)
        percentage_non_events = (non_events_in_category + 1) / (total_non_events + 1)
        
        # Calculate WOE and IV for the category
        woe = np.log(percentage_non_events / percentage_events)
        iv += (percentage_non_events - percentage_events) * woe
        
        # Store WOE value in the dictionary
        woe_dict[category] = woe

    return woe_dict, iv
#_____________________________________________________________________________________________FUNCTION-END

df_1 = dh.copy() 

# Calculate WOE and IV for the "STATE" variable
woe_dict, iv = calculate_woe_iv(df_1, 'STATE', 'ROOPYA_CUSTOMER_STATUS')

# Print the WOE values and IV for each category
print("WOE Dictionary:")
print(woe_dict)
print("Information Value (IV):", iv)

# Transform the "STATE" variable using the WOE dictionary
df_1['STATE_WOE'] = df_1['STATE'].map(woe_dict)

'''_______________________________________________Dictionary OF WOE___
WOE Dictionary:
{'MH ': -0.0415749986799236, 'GJ ': -0.2235031497688723, 'WB ': 0.2653150732625036, 'MP ': 0.5279443172982753, 'DL ': 0.10395221601077305, 'AP ': -0.12429389809857178, 'KA ': -0.17172343578496133, 'CG ': 0.6171177248656481, 'PB ': 0.254228916136814, 'UP ': -0.049344816103512115, 'CH ': -0.5735800293391802, 'HR ': -0.3569758233039565, 'TN ': 0.13989460862033512, 'KL ': 0.2389299660366667, 'RJ ': 0.24910008388707733, 'UK ': 0.29293131332384853, 'BR ': 0.0977083574082658, 'JH ': 0.16798905907964595, 'HP ': 0.25125861692328033, 'GA ': -0.15886416145886503, 'OR ': 0.12510733159638007, 'AS ': 0.4008946163960121, 'ML ': 0.4252119240467184, 'JK ': 1.3526759565189987, 'PY ': 0.24289036725276353, 'DN ': -0.32620416463720275, 'AN ': 2.197168765978594, 'DD ': 0.8256894906438438, 'MN ': 1.9858596723113868, 'SK ': 1.8249293054987494, 'AR ': 2.3223319089325996, 'TR ': 1.1802345083247512, 'NL ': 1.939339656676494, 'MZ ': 3.2386226408067547, 'LD ': 3.644087748914919}
'''

dh_1 = df_1.copy()


#----------------------------------------------------------------------------------------------------EXATLY COlumns
'''Index(['CREDIT_REPORT_ID', 'ACCOUNT_TYPE_Auto_Loan',
       'ACCOUNT_TYPE_Credit_Card', 'ACCOUNT_TYPE_Home_Loan',
       'ACCOUNT_TYPE_Personal_Loan', 'PRIMARY_NO_OF_ACCOUNTS',
       'PRIMARY_ACTIVE_ACCOUNTS', 'PRIMARY_OVERDUE_ACCOUNTS',
       'PRIMARY_CURRENT_BALANCE', 'PRIMARY_SANCTIONED_AMOUNT',
       'PRIMARY_DISTRIBUTED_AMOUNT', 'PRIMARY_INSTALLMENT_AMOUNT',
       'NEW_ACCOUNTS_IN_LAST_SIX_MONTHS', 'AVERAGE_ACCOUNT_AGE',
       'NO_OF_INQUIRIES', 'CURRENT_BALANCE', 'OVERDUE_AMOUNT', 'INCOME',
       'AGE_COHORT', 'CONTRIBUTOR_TYPE_NBF', 'CONTRIBUTOR_TYPE_PRB',
       'OWNERSHIP_IND_Guarantor', 'OWNERSHIP_IND_Individual',
       'OWNERSHIP_IND_Joint', 'OWNERSHIP_IND_Supl_Card_Holder',
       'ACCOUNT_STATUS_Active', 'ACCOUNT_STATUS_Closed',
       'Active_CURRENT_BALANCE', 'Closed_CURRENT_BALANCE',
       'Active_OVERDUE_AMOUNT', 'Closed_OVERDUE_AMOUNT',
       'ROOPYA_CUSTOMER_STATUS', 'STATE_WOE']'''

#----------------------------------------train_test_data-----
# Define your features (X_dh_1) and target variable (y_dh_1)
X_dh_1 = dh_1.drop(columns=['CREDIT_REPORT_ID', 'ROOPYA_CUSTOMER_STATUS'])  # Exclude non-numeric and target columns
y_dh_1 = dh_1['ROOPYA_CUSTOMER_STATUS']  # Target variable


#--------------------------------------------------------------------------------------------------------------StandardScalar
# Standardize features (optional but often recommended for logistic regression)
from sklearn.preprocessing import StandardScaler
scaler = StandardScaler()
X_train_dh_1 = scaler.fit_transform(X_train_dh_1)
X_test_dh_1 = scaler.transform(X_test_dh_1)


#--------------------------------------------------------------------------------------------------ADDRESS_Imbalance Data-----
from imblearn.over_sampling import SMOTE
# Apply SMOTE to the training data
smote = SMOTE(random_state=42)
X_train_resampled, y_train_resampled = smote.fit_resample(X_train_dh_1, y_train_dh_1)
# Apply the same transformation to the testing data
X_test_resampled, y_test_resampled = smote.fit_resample(X_test_dh_1, y_test_dh_1)

# Create and train the logistic regression model on the resampled data
model_dh_1_smote = LogisticRegression(penalty='l2', C=1.0)
model_dh_1_smote.fit(X_train_resampled, y_train_resampled)

dh = df[df['ACCOUNT_TYPE_Home_Loan'] != 0]     # for home loan 


#------------------------------------------------------------------------------Handling Null Values-------------------------------------
from sklearn.impute import SimpleImputer
# handling with mean 
dh['AGE_COHORT'].fillna(dh['AGE_COHORT'].mean(), inplace=True)


#--------------------------------------------------------------------------------FEATURE ENGENNEEING----------------------------------------------------------
# Convert AVERAGE_ACCOUNT_AGE to the total number of months
def convert_to_months(age_str):
    years, months = age_str.split(' ')
    total_months = int(years.strip('yrs')) * 12 + int(months.strip('mon'))
    return total_months

dh['AVERAGE_ACCOUNT_AGE'] = dh['AVERAGE_ACCOUNT_AGE'].apply(convert_to_months)


#----------------------------------------------------------------------------------------CONVERt into Binary Vlues, Good:1, Bad:0--------------------------------
# Assuming 'Roopya_customer_Status' is a column in your DataFrame
dh['ROOPYA_CUSTOMER_STATUS'] = dh['ROOPYA_CUSTOMER_STATUS'].map({'Good': 1, 'Bad': 0})


#------------------------------------------------------------------------------------Planning With WOE, weight of evidence--------------------
# Define a function to calculate WOE and IV
#______________________________________________________________________________ ___________FUNTION-START
def calculate_woe_iv(df, categorical_column, target_column):
    # Calculate the total count of events and non-events
    
    total_events = df[target_column].sum()
    total_non_events = df.shape[0] - total_events
    
    woe_dict = {}
    iv = 0
    
    # Loop through unique categories in the categorical column
    for category in df[categorical_column].unique():
        category_df = df[df[categorical_column] == category]
        events_in_category = category_df[target_column].sum()
        non_events_in_category = category_df.shape[0] - events_in_category
        
        # Calculate percentage of events and non-events in the category
        percentage_events = (events_in_category + 1) / (total_events + 1)
        percentage_non_events = (non_events_in_category + 1) / (total_non_events + 1)
        
        # Calculate WOE and IV for the category
        woe = np.log(percentage_non_events / percentage_events)
        iv += (percentage_non_events - percentage_events) * woe
        
        # Store WOE value in the dictionary
        woe_dict[category] = woe

    return woe_dict, iv
#_____________________________________________________________________________________________FUNCTION-END

df_1 = dh.copy() 

# Calculate WOE and IV for the "STATE" variable
woe_dict, iv = calculate_woe_iv(df_1, 'STATE', 'ROOPYA_CUSTOMER_STATUS')

# Print the WOE values and IV for each category
print("WOE Dictionary:")
print(woe_dict)
print("Information Value (IV):", iv)

# Transform the "STATE" variable using the WOE dictionary
df_1['STATE_WOE'] = df_1['STATE'].map(woe_dict)

'''_______________________________________________Dictionary OF WOE___
WOE Dictionary:
{'MH ': -0.0415749986799236, 'GJ ': -0.2235031497688723, 'WB ': 0.2653150732625036, 'MP ': 0.5279443172982753, 'DL ': 0.10395221601077305, 'AP ': -0.12429389809857178, 'KA ': -0.17172343578496133, 'CG ': 0.6171177248656481, 'PB ': 0.254228916136814, 'UP ': -0.049344816103512115, 'CH ': -0.5735800293391802, 'HR ': -0.3569758233039565, 'TN ': 0.13989460862033512, 'KL ': 0.2389299660366667, 'RJ ': 0.24910008388707733, 'UK ': 0.29293131332384853, 'BR ': 0.0977083574082658, 'JH ': 0.16798905907964595, 'HP ': 0.25125861692328033, 'GA ': -0.15886416145886503, 'OR ': 0.12510733159638007, 'AS ': 0.4008946163960121, 'ML ': 0.4252119240467184, 'JK ': 1.3526759565189987, 'PY ': 0.24289036725276353, 'DN ': -0.32620416463720275, 'AN ': 2.197168765978594, 'DD ': 0.8256894906438438, 'MN ': 1.9858596723113868, 'SK ': 1.8249293054987494, 'AR ': 2.3223319089325996, 'TR ': 1.1802345083247512, 'NL ': 1.939339656676494, 'MZ ': 3.2386226408067547, 'LD ': 3.644087748914919}
'''

dh_1 = df_1.copy()


#----------------------------------------------------------------------------------------------------EXATLY COlumns
'''Index(['CREDIT_REPORT_ID', 'ACCOUNT_TYPE_Auto_Loan',
       'ACCOUNT_TYPE_Credit_Card', 'ACCOUNT_TYPE_Home_Loan',
       'ACCOUNT_TYPE_Personal_Loan', 'PRIMARY_NO_OF_ACCOUNTS',
       'PRIMARY_ACTIVE_ACCOUNTS', 'PRIMARY_OVERDUE_ACCOUNTS',
       'PRIMARY_CURRENT_BALANCE', 'PRIMARY_SANCTIONED_AMOUNT',
       'PRIMARY_DISTRIBUTED_AMOUNT', 'PRIMARY_INSTALLMENT_AMOUNT',
       'NEW_ACCOUNTS_IN_LAST_SIX_MONTHS', 'AVERAGE_ACCOUNT_AGE',
       'NO_OF_INQUIRIES', 'CURRENT_BALANCE', 'OVERDUE_AMOUNT', 'INCOME',
       'AGE_COHORT', 'CONTRIBUTOR_TYPE_NBF', 'CONTRIBUTOR_TYPE_PRB',
       'OWNERSHIP_IND_Guarantor', 'OWNERSHIP_IND_Individual',
       'OWNERSHIP_IND_Joint', 'OWNERSHIP_IND_Supl_Card_Holder',
       'ACCOUNT_STATUS_Active', 'ACCOUNT_STATUS_Closed',
       'Active_CURRENT_BALANCE', 'Closed_CURRENT_BALANCE',
       'Active_OVERDUE_AMOUNT', 'Closed_OVERDUE_AMOUNT',
       'ROOPYA_CUSTOMER_STATUS', 'STATE_WOE']'''

#----------------------------------------train_test_data-----
# Define your features (X_dh_1) and target variable (y_dh_1)
X_dh_1 = dh_1.drop(columns=['CREDIT_REPORT_ID', 'ROOPYA_CUSTOMER_STATUS'])  # Exclude non-numeric and target columns
y_dh_1 = dh_1['ROOPYA_CUSTOMER_STATUS']  # Target variable


#--------------------------------------------------------------------------------------------------------------StandardScalar
# Standardize features (optional but often recommended for logistic regression)
from sklearn.preprocessing import StandardScaler
scaler = StandardScaler()
X_train_dh_1 = scaler.fit_transform(X_train_dh_1)
X_test_dh_1 = scaler.transform(X_test_dh_1)


#--------------------------------------------------------------------------------------------------ADDRESS_Imbalance Data-----
from imblearn.over_sampling import SMOTE
# Apply SMOTE to the training data
smote = SMOTE(random_state=42)
X_train_resampled, y_train_resampled = smote.fit_resample(X_train_dh_1, y_train_dh_1)
# Apply the same transformation to the testing data
X_test_resampled, y_test_resampled = smote.fit_resample(X_test_dh_1, y_test_dh_1)

# Create and train the logistic regression model on the resampled data
model_dh_1_smote = LogisticRegression(penalty='l2', C=1.0)
model_dh_1_smote.fit(X_train_resampled, y_train_resampled)


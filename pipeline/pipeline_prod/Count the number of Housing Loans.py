import numpy as np
import pandas as pd

df = pd.read_json('C:\Users\swarn\OneDrive\Documents\Roopya Transfer\Roopya_DE\Latest JSON instances.json5')

with open('C:\Users\swarn\OneDrive\Documents\Roopya Transfer\Roopya_DE\Latest JSON instances.json5', 'r') as file:
    data = json.load(file)

columns = ['Housing Loan', 'Property Loan', 'Leasing', 'Microfinance Housing Loan']

count = 0

# Iterate through the data and count occurrences of columns
for item in data:
    for col in columns:
        if col in item:
            count += 1

# Print the count
print(f"Count of encountered columns: {count}")

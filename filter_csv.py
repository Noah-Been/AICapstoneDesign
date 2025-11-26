import pandas as pd

# Read the CSV file
df = pd.read_csv('KOSPI_KOSDAQ.csv', dtype={'Code': str})

# Sort the DataFrame by 'Code' column in ascending order
df_sorted = df.sort_values(by='Code', ascending=True)

# Save the sorted DataFrame to a new CSV file
output_filename = 'KOSPI_KOSDAQ_sorted.csv'
df_sorted.to_csv(output_filename, index=False)

# Display the first few rows of the sorted DataFrame
print(df_sorted.head())

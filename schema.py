import pandas as pd

# Load the schema CSV file
schema_df = pd.read_csv("schema.csv")

# Display the first few rows
print(schema_df.info())

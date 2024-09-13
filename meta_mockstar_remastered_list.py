import pandas as pd
import numpy as np
import json

# Load metadata from JSON file
with open('metadata.json', 'r') as f:
    metadata = json.load(f)

# Function to generate tables based on metadata
def generate_table(meta, table_name):
    num_records = meta[table_name]['num_records']
    columns = {}
    for col_name, col_expr in meta[table_name]['columns'].items():
        if isinstance(col_expr, list):
            # Handle list of values directly
            columns[col_name] = np.random.choice(col_expr, size=num_records)
        else:
            # Evaluate the expression for generating data
            columns[col_name] = eval(col_expr)
    return pd.DataFrame(columns)

# Generate dimension tables
customers = generate_table(metadata, 'customers')
products = generate_table(metadata, 'products')
dates = generate_table(metadata, 'dates')

# Generate fact table
fact_table = generate_table(metadata, 'fact')

# Join fact table with dimension tables
fact_with_customers = fact_table.merge(customers, on='CustomerID')
fact_with_full_data = fact_with_customers.merge(products, on='ProductID').merge(dates, on='DateKey')

# Display the generated data
print("Customer Dimension Table:")
print(customers.head())
print("\nProduct Dimension Table:")
print(products.head())
print("\nDate Dimension Table:")
print(dates.head())
print("\nFact Table:")
print(fact_table.head())
print("\nFact Table with Full Data:")
print(fact_with_full_data.head())
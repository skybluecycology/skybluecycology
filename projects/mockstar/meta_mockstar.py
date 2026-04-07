import pandas as pd
from faker import Faker
import random

# Initialize Faker
fake = Faker()

# Generate dimension tables
def generate_product_dimension(n):
    products = []
    for _ in range(n):
        products.append({
            'product_id': fake.unique.random_int(min=1, max=1000),
            'product_name': fake.word(),
            'category': fake.word()
        })
    return pd.DataFrame(products)

def generate_store_dimension(n):
    stores = []
    for _ in range(n):
        stores.append({
            'store_id': fake.unique.random_int(min=1, max=1000),
            'store_name': fake.company(),
            'location': fake.city()
        })
    return pd.DataFrame(stores)

def generate_date_dimension(start_date, end_date):
    dates = pd.date_range(start=start_date, end=end_date).to_frame(index=False, name='date')
    dates['year'] = dates['date'].dt.year
    dates['month'] = dates['date'].dt.month
    dates['day'] = dates['date'].dt.day
    dates['quarter'] = dates['date'].dt.quarter
    return dates

# Generate fact table
def generate_sales_fact_table(n, products, stores, dates):
    sales = []
    for _ in range(n):
        sales.append({
            'sale_id': fake.unique.random_int(min=1, max=100000),
            'product_id': random.choice(products['product_id']),
            'store_id': random.choice(stores['store_id']),
            'date': random.choice(dates['date']),
            'quantity_sold': random.randint(1, 100),
            'sales_amount': round(random.uniform(10.0, 1000.0), 2)
        })
    return pd.DataFrame(sales)

# Generate data
num_products = 10
num_stores = 5
num_sales = 1000
start_date = '2023-01-01'
end_date = '2023-12-31'

products = generate_product_dimension(num_products)
stores = generate_store_dimension(num_stores)
dates = generate_date_dimension(start_date, end_date)
sales = generate_sales_fact_table(num_sales, products, stores, dates)

# Display the data
print("Products Dimension Table:")
print(products.head())
print("\nStores Dimension Table:")
print(stores.head())
print("\nDates Dimension Table:")
print(dates.head())
print("\nSales Fact Table:")
print(sales.head())

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
from functools import reduce

# Initialize Spark Session
spark = SparkSession.builder.appName("CompareDatasets").getOrCreate()

# Load DataFrames from Databricks SQL extracts
df1 = spark.sql("SELECT * FROM database.table1")
df2 = spark.sql("SELECT * FROM database.table2")

# Define the key columns for join
key_columns = ["id", "another_key_column"]  # Update with your actual key columns

# Join condition
join_condition = reduce(lambda x, y: x & y, [df1[key] == df2[key] for key in key_columns])

# Full outer join on the key columns to align records
joined_df = df1.alias("df1").join(df2.alias("df2"), on=join_condition, how="full_outer")

# Get the list of columns to compare (excluding the key columns)
columns_to_compare = [col for col in df1.columns if col not in key_columns]

# Create comparison conditions dynamically for all columns
conditions = [
    (col(f"df1.{col_name}") != col(f"df2.{col_name}")) |
    col(f"df1.{col_name}").isNull() | col(f"df2.{col_name}").isNull()
    for col_name in columns_to_compare
]

# Combine conditions to filter rows where differences exist
difference_filter = reduce(lambda x, y: x | y, conditions)

# Apply filter to get differences
differences = joined_df.filter(difference_filter)

# Show differences
differences.show(truncate=False)

# Optionally, save the differences to a table or file
differences.write.format("parquet").save("/path/to/differences")

# Stop the Spark session
spark.stop()
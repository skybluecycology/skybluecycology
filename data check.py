from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

# Initialize Spark Session
spark = SparkSession.builder.appName("CompareDatasets").getOrCreate()

# Load DataFrames from Databricks SQL extracts
df1 = spark.sql("SELECT * FROM database.table1")
df2 = spark.sql("SELECT * FROM database.table2")

# Define the key column for join
key_column = "id"

# Full outer join on the key column to align records
joined_df = df1.alias("df1").join(df2.alias("df2"), df1[key_column] == df2[key_column], "full_outer")

# Get the list of columns
columns = df1.columns

# Create comparison conditions dynamically for all columns
conditions = [
    (col(f"df1.{col}") != col(f"df2.{col}")) | col(f"df1.{col}").isNull() | col(f"df2.{col}").isNull()
    for col in columns
]

# Combine conditions to filter rows where differences exist
differences = joined_df.filter(
    lit(False).select(lit(True) if cond else lit(False) for cond in conditions)
)

# Show differences
differences.show(truncate=False)

# Optionally, save the differences to a table or file
differences.write.format("parquet").save("/path/to/differences")

# Stop the Spark session
spark.stop()
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def create_spark_session(app_name="PandasPySparkIntegration"):
    """Create a PySpark session."""
    return SparkSession.builder.appName(app_name).getOrCreate()

def read_data(spark, filepath, file_format="csv", header=True):
    """Read data into a PySpark DataFrame."""
    return spark.read.format(file_format).option("header", header).load(filepath)

def write_data(data, filepath, file_format="parquet"):
    """Write a PySpark DataFrame to a file."""
    data.write.format(file_format).save(filepath)

def filter_data(data, condition):
    """Filter data based on a given condition."""
    return data.filter(condition)

def group_by(data, columns):
    """Group data by one or more columns."""
    return data.groupBy(columns)

def aggregate_data(grouped_data, aggregation_functions):
    """Perform aggregation operations on grouped data."""
    return grouped_data.agg(aggregation_functions)

def join_data(data1, data2, join_condition):
    """Join two DataFrames based on a specified join condition."""
    return data1.join(data2, join_condition)

def pivot_data(data, pivot_column, values_column):
    """Pivot data to create a pivot table."""
    return data.pivot(pivot_column).agg(first(values_column))

def calculate_statistics(data, columns):
    """Calculate summary statistics for specified columns in a DataFrame."""
    return data.select(columns).describe()

def fill_missing_values(data, strategy="mean"):
    """Fill missing values in a DataFrame using specified strategies (e.g., mean, median)."""
    return data.fillna(strategy)

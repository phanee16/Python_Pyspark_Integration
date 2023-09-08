import pandas as pd

def load_data(filepath):
    """Load data from a CSV file into a Pandas DataFrame."""
    return pd.read_csv(filepath)

def save_data(data, filepath):
    """Save a Pandas DataFrame to a CSV file."""
    data.to_csv(filepath, index=False)

def filter_data(data, condition):
    """Filter data based on a given condition."""
    return data[condition]

def group_by(data, columns):
    """Group data by one or more columns."""
    return data.groupby(columns)

def aggregate_data(grouped_data, aggregation_functions):
    """Perform aggregation operations on grouped data."""
    return grouped_data.agg(aggregation_functions)

def merge_data(data1, data2, on_columns):
    """Merge two DataFrames based on specified columns."""
    return pd.merge(data1, data2, on=on_columns)

def pivot_data(data, index, columns, values):
    """Pivot data to create a pivot table."""
    return data.pivot_table(index=index, columns=columns, values=values)

def calculate_statistics(data):
    """Calculate summary statistics for a DataFrame."""
    return data.describe()

def fill_missing_values(data, method='mean'):
    """Fill missing values in a DataFrame using specified methods (e.g., mean, median)."""
    if method == 'mean':
        return data.fillna(data.mean())
    elif method == 'median':
        return data.fillna(data.median())

def drop_columns(data, columns_to_drop):
    """Drop specified columns from a DataFrame."""
    return data.drop(columns=columns_to_drop)

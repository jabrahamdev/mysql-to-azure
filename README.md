# Guide to Ingest MySQL Database into Azure Databricks as Parquet

### Introduction

This comprehensive guide will walk you through the process of ingesting a MySQL database into Azure Databricks as Parquet format. Azure Databricks is a powerful data analytics platform that enables efficient processing and analysis of large datasets. By converting the MySQL database into Parquet format, we can leverage the benefits of columnar storage and optimize query performance in Databricks.

The process involves the following steps:

- Establishing a connection to the MySQL database
- Extracting data from MySQL tables
- Transforming and cleaning the data
- Applying advanced transformations and aggregations
- Writing the transformed data as Parquet files in Azure Databricks

### Prerequisites

Before proceeding, ensure you have the following:

- Azure Databricks workspace
- MySQL database with appropriate credentials
- Databricks cluster with required libraries (e.g., mysql-connector-java)


## Establish Connection to MySQL Database

To connect to the MySQL database, we need to configure the necessary connection details. We will use the mysql-connector-python library to establish the connection.


```python
import mysql.connector

# MySQL connection configuration
mysql_config = {
    'user': 'your_username',
    'password': 'your_password',
    'host': 'your_host',
    'database': 'your_database'
}

# Establish connection
conn = mysql.connector.connect(**mysql_config)
```

## Extract Data from MySQL Tables

Now that we have established the connection, we can proceed to extract data from the MySQL tables. We will use the pandas library to retrieve data in a tabular format.

```python
import pandas as pd

# Define table names and column names to extract
table_column_map = {
    'table1': ['column1', 'column2', 'column3'],
    'table2': ['column4', 'column5'],
    'table3': ['column6', 'column7', 'column8']
}

# Extract data from MySQL tables
dfs = {}
for table, columns in table_column_map.items():
    query = f'SELECT {", ".join(columns)} FROM {table}'
    df = pd.read_sql(query, conn)
    dfs[table] = df
```

## Transform and Clean the Data

At this stage, you can perform various complex data transformations and cleaning operations on the extracted data. Let's look at some examples:


```python
# Example transformation: Apply a custom function to a column
def custom_function(value):
    # Perform complex transformation logic
    transformed_value = ...
    return transformed_value

dfs['table1']['transformed_column'] = dfs['table1']['column1'].apply(custom_function)

# Example transformation: Split a column into multiple columns
dfs['table2'][['column4_part1', 'column4_part2']] = dfs['table2']['column4'].str.split('-', expand=True)

# Example cleaning: Remove rows with missing values
dfs['table3'].dropna(inplace=True)

# Example transformation: Apply a mathematical function to a column
import numpy as np

dfs['table3']['column8_sqrt'] = np.sqrt(dfs['table3']['column8'])

# Example transformation: Convert a column to categorical
dfs['table3']['column6'] = dfs['table3']['column6'].astype('category')

```

## Apply Advanced Transformations and Aggregations

In this step, we can apply more complex transformations and aggregations on the extracted data. Let's demonstrate some examples:

```python
# Example transformation: Apply a lambda function to a column
dfs['table1']['transformed_column'] = dfs['table1']['column1'].apply(lambda x: x.upper())

# Example aggregation: Group by a column and calculate sum and count
grouped_data = dfs['table2'].groupby('column4').agg({'column5': ['sum', 'count']})

```

## Write Transformed Data as Parquet

Once the data is transformed, we can write it as Parquet files in Azure Databricks. We will use the pyarrow library to achieve this.

```python
import pyarrow as pa
import pyarrow.parquet as pq

# Define the Parquet file path
output_path = '/mnt/parquet/output.parquet'

# Convert pandas DataFrames to PyArrow Tables
tables = [pa.Table.from_pandas(df) for df in dfs.values()]

# Write PyArrow Tables as Parquet files
pq.write_table(tables, output_path)

```


### FULL CODE


```python
import mysql.connector
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Step 1: Establish Connection to MySQL Database

# MySQL connection configuration
mysql_config = {
    'user': 'your_username',
    'password': 'your_password',
    'host': 'your_host',
    'database': 'your_database'
}

# Establish connection
conn = mysql.connector.connect(**mysql_config)


# Step 2: Extract Data from MySQL Tables

# Define table names and column names to extract
table_column_map = {
    'table1': ['column1', 'column2', 'column3'],
    'table2': ['column4', 'column5'],
    'table3': ['column6', 'column7', 'column8']
}

# Extract data from MySQL tables
dfs = {}
for table, columns in table_column_map.items():
    query = f'SELECT {", ".join(columns)} FROM {table}'
    df = pd.read_sql(query, conn)
    dfs[table] = df


# Step 3: Transform and Clean the Data

# Example transformation: Apply a custom function to a column
def custom_function(value):
    # Perform complex transformation logic
    transformed_value = ...
    return transformed_value

dfs['table1']['transformed_column'] = dfs['table1']['column1'].apply(custom_function)

# Example transformation: Split a column into multiple columns
dfs['table2'][['column4_part1', 'column4_part2']] = dfs['table2']['column4'].str.split('-', expand=True)

# Example cleaning: Remove rows with missing values
dfs['table3'].dropna(inplace=True)

# Example transformation: Apply a mathematical function to a column
import numpy as np

dfs['table3']['column8_sqrt'] = np.sqrt(dfs['table3']['column8'])

# Example transformation: Convert a column to categorical
dfs['table3']['column6'] = dfs['table3']['column6'].astype('category')


# Step 4: Apply Advanced Transformations and Aggregations

# Example transformation: Apply a lambda function to a column
dfs['table1']['transformed_column'] = dfs['table1']['column1'].apply(lambda x: x.upper())

# Example aggregation: Group by a column and calculate sum and count
grouped_data = dfs['table2'].groupby('column4').agg({'column5': ['sum', 'count']})


# Step 5: Write Transformed Data as Parquet

# Define the Parquet file path
output_path = '/mnt/parquet/output.parquet'

# Convert pandas DataFrames to PyArrow Tables
tables = [pa.Table.from_pandas(df) for df in dfs.values()]

# Write PyArrow Tables as Parquet files
pq.write_table(tables, output_path)


```

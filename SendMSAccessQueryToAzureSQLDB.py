"""
.Description: Migrate data from access database to Azure sQL DB.

.Version:
-Original Release Steve On - 20240711
"""

import pyodbc
import pandas as pd

# Define the path to your Access database
database_path = r'C:\Folder\MSAccessDatabaseFile.accdb'

# MS Access database connection string
ms_access_conn_str = (
        r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
        r'DBQ=' + database_path + ';'
)
# Azure SQL Server database connection string
azure_conn_str = (
    "Driver={ODBC Driver 17 for SQL Server};"
    "Server=azureServerName.database.windows.net;"
    "Database=lct_hs;"
    "Uid=usrId;"
    "Pwd=!password;"
)

# Establish a connection to the database
ms_access_conn = pyodbc.connect(ms_access_conn_str)
azure_conn = pyodbc.connect(azure_conn_str)

# Create a cursor object
ms_access_cursor = ms_access_conn.cursor()
azure_cursor = azure_conn.cursor()

# Define your SQL query
ms_access_query = ("SELECT columnProperty, anotherColumnProperty, dateTimeProperty, isColumnProperty FROM dbTable WHERE columnProperty > 122")

# Execute the query and fetch the results
ms_access_cursor.execute(ms_access_query)
ms_access_rows = ms_access_cursor.fetchall()

# Fetch column names
ms_access_columns = [column[0] for column in ms_access_cursor.description]

# Load the data into a pandas DataFrame
ms_access_df = pd.DataFrame.from_records(ms_access_rows, columns=ms_access_columns)

# Define the mapping of Access columns to Azure columns and their target data types
column_mapping = {
    'columnProperty': ('columnProperty', 'int'),
    'anotherColumnProperty': ('anotherColumnProperty', 'str'),
    'dateTimeProperty': ('dateTimeProperty', 'datetime'),
    'isColumnProperty': ('isColumnProperty', 'bool')
}

# Function to convert data types
def convert_type(value, target_type):
    if pd.isna(value):
        return None
    if target_type == 'str':
        return str(value)
    elif target_type == 'int':
        return int(value)
    elif target_type == 'float':
        return float(value)
    elif target_type == 'datetime':
        return pd.to_datetime(value)
    elif target_type == 'bool':
        return bool(value)
    else:
        return value


# Enable IDENTITY_INSERT for the table
# azure_cursor.execute("SET IDENTITY_INSERT dbTable ON")

# Prepare the insert statement
azure_columns = ', '.join([mapping[0] for mapping in column_mapping.values()])
placeholders = ', '.join(['?'] * len(column_mapping))
sql = f"INSERT INTO dbTable ({azure_columns}) VALUES ({placeholders})"

# Convert the data and prepare for batch insert
values_list = []
for index, row in ms_access_df.iterrows():
    values = [convert_type(row[access_col], column_mapping[access_col][1]) for access_col in column_mapping.keys()]
    values_list.append(values)

# Insert data into Azure SQL Database in batches
batch_size = 1000  # Adjust the batch size as needed
for i in range(0, len(values_list), batch_size):
    batch = values_list[i:i + batch_size]
    azure_cursor.executemany(sql, batch)

# Disable IDENTITY_INSERT for the table
# azure_cursor.execute("SET IDENTITY_INSERT dbTable OFF")

# Commit the transaction
azure_conn.commit()

# Close the connection
ms_access_conn.close()
azure_conn.close()

# Display the DataFrame
print(ms_access_df)
print("Data copied successfully from Access database to Azure SQL Database.")

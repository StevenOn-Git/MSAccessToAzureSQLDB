# MSAccessToAzureSQLDB
Python Script to move Data from MS Access to Azure SQL DB

First you need to make sure you have your ODBC connection installed and it contains the ability to connect to Microsoft Access tables. Additionally, make sure to unblock the accdb file locally from the file explorer properties. 

Code Walkthrough:
1. Setup your connection strings to MS Access and Azure.
2. Build a cursor for each system.
3. Build a query from MS Access.
4. Perform a column data type assignment.
5. Batch and insert the MS Access SQL query into Azure SQL DB.
6. Print a pandas data frame if successful. 

# DataSpark-Illuminating-Insights-for-Global-Electronics
This project involves processing sales data from Global Electronics, transforming it, and storing it in a MySQL database. The data includes customers, exchange rates, products, sales, and store information. After processing and storing the data, the project exports the data back into CSV files for further analysis or archival purposes.

Prerequisites
Ensure you have the following installed:

Python 3.x
Pandas library
PyMySQL library
MySQL server
You can install the required Python libraries using pip:

pip install pandas pymysql
Data Files
The project uses the following CSV files as input:

Customers.csv: Contains customer information, including demographics and geographic details.
Exchange_Rates.csv: Contains historical exchange rates for different currencies.
Products.csv: Lists products available for sale, including cost and pricing information.
Sales.csv: Contains sales transaction records, including order and delivery details.
Stores.csv: Contains information about store locations and sizes.
Steps Performed
1. Load Data
The script reads data from CSV files using Pandas:


customers_df = pd.read_csv('/path/to/Customers.csv', encoding='latin1')
exchange_rates_df = pd.read_csv('/path/to/Exchange_Rates.csv', encoding='latin1')
products_df = pd.read_csv('/path/to/Products.csv', encoding='latin1')
sales_df = pd.read_csv('/path/to/Sales.csv', encoding='latin1')
stores_df = pd.read_csv('/path/to/Stores.csv', encoding='latin1')

2. Data Cleaning and Transformation
Customers: Missing state codes are filled with 'Unknown'. Birthdays are converted to the YYYY-MM-DD format.

Sales: Missing delivery dates are filled with the average delivery time calculated from the available data. The order and delivery dates are converted to datetime format, and delivery days are computed as the difference between these dates.

Products: Currency columns for unit cost and price are cleaned by removing dollar signs and converting them to float.

Stores: Rows with any null values are dropped. Open dates are converted to the YYYY-MM-DD format.

3. Database Operations
Connect to MySQL Database: Establish a connection to the MySQL server using PyMySQL.


connection = pymysql.connect(
    host='localhost',
    user='root',
    password='Digi08@Life',
    database='capstone2_Global_Electronics'
)
Create Tables: Create tables for customers, exchange rates, products, sales, and stores if they don't already exist.

Insert Data: Insert data from the DataFrames into the respective MySQL tables using SQL INSERT queries.

4. Export Data
Export to CSV: After inserting data into MySQL, export each table back to a CSV file using Pandas.


def save_table_to_csv(table_name, file_path):
    query = f'SELECT * FROM {table_name}'
    df = pd.read_sql(query, connection)
    df.to_csv(file_path, index=False)
Error Handling
The script includes basic error handling for MySQL operations to catch and display any exceptions that occur during database interactions.

File Paths
Ensure that the paths in the script match your local setup. Update file paths to point to the correct CSV files and export locations.

Conclusion
This script automates the process of reading data, transforming it, storing it in a MySQL database, and then exporting it. It is a flexible solution for managing sales and product data for Global Electronics, enabling data consistency and ease of analysis.

import pandas as pd
import pymysql

# Load data from CSV files
customers_df = pd.read_csv('/Users/sanjay/Downloads/Customers.csv', encoding='latin1')
exchange_rates_df = pd.read_csv('/Users/sanjay/Downloads/Exchange_Rates.csv', encoding='latin1')
products_df = pd.read_csv('/Users/sanjay/Downloads/Products.csv', encoding='latin1')
sales_df = pd.read_csv('/Users/sanjay/Downloads/Sales.csv', encoding='latin1')
stores_df = pd.read_csv('/Users/sanjay/Downloads/Stores.csv', encoding='latin1')

# Fill null values in 'State Code' with a specific value, e.g., 'Unknown'
customers_df['State Code'].fillna('Unknown', inplace=True)

# Convert 'Order Date' and 'Delivery Date' to datetime format
sales_df['Order Date'] = pd.to_datetime(sales_df['Order Date'], format='%m/%d/%Y')
sales_df['Delivery Date'] = pd.to_datetime(sales_df['Delivery Date'], format='%m/%d/%Y', errors='coerce')

# Calculate delivery days only for rows where 'Delivery Date' is not NaN
sales_df['Delivery Days'] = (sales_df['Delivery Date'] - sales_df['Order Date']).dt.days

# Compute the average delivery days from rows with valid 'Delivery Days'
average_delivery_days = sales_df['Delivery Days'].dropna().mean()

# Fill missing 'Delivery Date' with the calculated average delivery days
sales_df['Delivery Date'] = sales_df.apply(
    lambda row: row['Order Date'] + pd.Timedelta(days=average_delivery_days)
    if pd.isnull(row['Delivery Date'])
    else row['Delivery Date'],
    axis=1
)

# Extract only the date component
sales_df['Order Date'] = sales_df['Order Date'].dt.date
sales_df['Delivery Date'] = sales_df['Delivery Date'].dt.date

# Calculate delivery days where Delivery Days is NaN
sales_df['Delivery Days'] = sales_df.apply(
    lambda row: (row['Delivery Date'] - row['Order Date']).days
    if pd.isnull(row['Delivery Days']) and pd.notnull(row['Delivery Date'])
    else row['Delivery Days'],
    axis=1
)

# Drop rows with any null values in the stores DataFrame
stores_df.dropna(inplace=True)

# Convert 'Birthday' to datetime format
customers_df['Birthday'] = pd.to_datetime(customers_df['Birthday'], format='%m/%d/%Y').dt.strftime('%Y-%m-%d')

# Convert date columns to the correct format
def convert_dates(df, date_columns):
    for column in date_columns:
        if column in df.columns:
            df[column] = pd.to_datetime(df[column], errors='coerce').dt.strftime('%Y-%m-%d')
    return df

customers_df = convert_dates(customers_df, ['Birthday'])
exchange_rates_df = convert_dates(exchange_rates_df, ['Date'])
products_df = convert_dates(products_df, [])  # Assuming no date columns
sales_df = convert_dates(sales_df, ['Order Date', 'Delivery Date'])
stores_df = convert_dates(stores_df, ['Open Date'])

# Function to clean and convert currency columns
def clean_currency_column(df, column_name):
    if column_name in df.columns:
        # Remove dollar signs and extra spaces, then convert to numeric
        df[column_name] = df[column_name].replace('[\$,]', '', regex=True).astype(float)
    return df

# Clean the currency columns in products_df
products_df = clean_currency_column(products_df, 'Unit Cost USD')
products_df = clean_currency_column(products_df, 'Unit Price USD')

# Database connection details
username = 'root'
password = 'Digi08@Life'
host = 'localhost'
database = 'capstone2_Global_Electronics'  # Replace with your MySQL database name

try:
    connection = pymysql.connect(
        host=host,
        user=username,
        password=password,
        database=database
    )

    with connection.cursor() as cursor:
        # Create and populate Customers table
        create_customers_table_query = '''
        CREATE TABLE IF NOT EXISTS Customers (
            CustomerKey INT PRIMARY KEY,
            Gender VARCHAR(50),
            Name VARCHAR(100),
            City VARCHAR(100),
            State_Code VARCHAR(100),
            State VARCHAR(100),
            Zip_Code VARCHAR(20),
            Country VARCHAR(50),
            Continent VARCHAR(50),
            Birthday DATE
        )
        '''
        cursor.execute(create_customers_table_query)

        for _, row in customers_df.iterrows():
            insert_customers_query = '''
            INSERT INTO Customers (CustomerKey, Gender, Name, City, State_Code, State, Zip_Code, Country, Continent, Birthday)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            '''
            cursor.execute(insert_customers_query, tuple(row))

        # Create and populate exchange_rates table
        create_exchange_rates_table_query = '''
        CREATE TABLE IF NOT EXISTS exchange_rates (
            Date DATE,
            Currency VARCHAR(10),
            Exchange DECIMAL(10, 4),
            PRIMARY KEY (Date, Currency)
        )
        '''
        cursor.execute(create_exchange_rates_table_query)

        for _, row in exchange_rates_df.iterrows():
            insert_exchange_rates_query = '''
            INSERT INTO exchange_rates (Date, Currency, Exchange)
            VALUES (%s, %s, %s)
            '''
            cursor.execute(insert_exchange_rates_query, tuple(row))

        # Create and populate products table
        create_products_table_query = '''
        CREATE TABLE IF NOT EXISTS products (
            ProductKey INT PRIMARY KEY,
            Product_Name VARCHAR(100),
            Brand VARCHAR(50),
            Color VARCHAR(20),
            Unit_Cost_USD DECIMAL(10, 2),
            Unit_Price_USD DECIMAL(10, 2),
            SubcategoryKey INT,
            Subcategory VARCHAR(100),
            CategoryKey INT,
            Category VARCHAR(100)
        )
        '''
        cursor.execute(create_products_table_query)

        for _, row in products_df.iterrows():
            insert_products_query = '''
            INSERT INTO products (ProductKey, Product_Name, Brand, Color, Unit_Cost_USD, Unit_Price_USD, SubcategoryKey, Subcategory, CategoryKey, Category)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            '''
            cursor.execute(insert_products_query, tuple(row))

        # Create and populate sales table
        create_sales_table_query = '''
        CREATE TABLE IF NOT EXISTS sales (
            Order_Number INT,
            Line_Item INT,
            Order_Date DATE,
            Delivery_Date DATE,
            CustomerKey INT,
            StoreKey INT,  -- Not linking with stores table
            ProductKey INT,
            Quantity INT,
            Currency_Code VARCHAR(10),
            Delivery_Days DECIMAL,
            PRIMARY KEY (Order_Number, Line_Item),
            FOREIGN KEY (CustomerKey) REFERENCES Customers (CustomerKey),
            FOREIGN KEY (ProductKey) REFERENCES products (ProductKey)
        )
        '''
        cursor.execute(create_sales_table_query)

        for _, row in sales_df.iterrows():
            insert_sales_query = '''
            INSERT INTO sales (Order_Number, Line_Item, Order_Date, Delivery_Date, CustomerKey, StoreKey, ProductKey, Quantity, Currency_Code, Delivery_Days)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            '''
            cursor.execute(insert_sales_query, tuple(row))

        # Create and populate stores table
        create_stores_table_query = '''
        CREATE TABLE IF NOT EXISTS stores (
            StoreKey INT PRIMARY KEY,
            Country VARCHAR(50),
            State VARCHAR(50),
            Square_Meters DECIMAL,
            Open_Date DATE
        )
        '''
        cursor.execute(create_stores_table_query)

        for _, row in stores_df.iterrows():
            insert_stores_query = '''
            INSERT INTO stores (StoreKey, Country, State, Square_Meters, Open_Date)
            VALUES (%s, %s, %s, %s, %s)
            '''
            cursor.execute(insert_stores_query, tuple(row))

    # Commit the changes
    connection.commit()

    # Function to fetch data from a table and save it to a CSV file
    def save_table_to_csv(table_name, file_path):
        query = f'SELECT * FROM {table_name}'
        df = pd.read_sql(query, connection)
        df.to_csv(file_path, index=False)

    # Save all tables to CSV
    save_table_to_csv('Customers', '/Users/sanjay/Documents/global electronics excel file/customers.csv')
    save_table_to_csv('exchange_rates', '/Users/sanjay/Documents/global electronics excel file/exchange_rates.csv')
    save_table_to_csv('products', '/Users/sanjay/Documents/global electronics excel file/products.csv')
    save_table_to_csv('sales', '/Users/sanjay/Documents/global electronics excel file/sales.csv')
    save_table_to_csv('stores', '/Users/sanjay/Documents/global electronics excel file/stores.csv')

except pymysql.MySQLError as e:
    print(f"An error occurred: {e}")

finally:
    if connection and not connection._closed:
        try:
            connection.close()
        except pymysql.MySQLError as e:
            print(f"Error closing connection: {e}")

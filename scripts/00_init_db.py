#!/usr/bin/env python
# coding: utf-8

# In[6]:


import mysql.connector
import csv
from mysql.connector import Error

# Database configuration
config = {
    'user': 'xxx',       
    'password': 'xxx',    
    'host': 'xxx',
    'raise_on_warnings': True
}

try:
    # Connect to MySQL server
    conn = mysql.connector.connect(**config)
    cursor = conn.cursor()

    # Drop and create database
    cursor.execute("CREATE DATABASE dw_project")
    cursor.execute("USE dw_project")


    
    # Create dim_customers table
    cursor.execute("""
        CREATE TABLE dim_customers (
            customer_key INT,
            customer_id INT,
            customer_number VARCHAR(50),
            first_name VARCHAR(50),
            last_name VARCHAR(50),
            country VARCHAR(50),
            marital_status VARCHAR(50),
            gender VARCHAR(50),
            birthdate DATE,
            create_date DATE
        )
    """)

    # Create dim_products table
    cursor.execute("""
        CREATE TABLE dim_products (
            product_key INT,
            product_id INT,
            product_number VARCHAR(50),
            product_name VARCHAR(50),
            category_id VARCHAR(50),
            category VARCHAR(50),
            subcategory VARCHAR(50),
            maintenance VARCHAR(50),
            cost INT,
            product_line VARCHAR(50),
            start_date DATE
        )
    """)

    # Create fact_sales table
    cursor.execute("""
        CREATE TABLE fact_sales (
            order_number VARCHAR(50),
            product_key INT,
            customer_key INT,
            order_date DATE,
            shipping_date DATE,
            due_date DATE,
            sales_amount INT,
            quantity TINYINT,
            price INT
        )
    """)

    # Function to load CSV data into table
    def load_csv_to_table(table_name, csv_path):
        # Truncate table
        cursor.execute(f"TRUNCATE TABLE {table_name}")
        
        # Read and insert CSV data
        with open(csv_path, 'r', encoding='utf-8') as csvfile:
            csv_reader = csv.reader(csvfile)
            next(csv_reader)  # Skip header row (equivalent to FIRSTROW = 2)
            
            # Prepare insert statement based on table
            if table_name == 'dim_customers':
                query = """
                    INSERT INTO dim_customers 
                    (customer_key, customer_id, customer_number, first_name, last_name, 
                    country, marital_status, gender, birthdate, create_date)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
            elif table_name == 'dim_products':
                query = """
                    INSERT INTO dim_products 
                    (product_key, product_id, product_number, product_name, category_id,
                    category, subcategory, maintenance, cost, product_line, start_date)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
            elif table_name == 'fact_sales':
                query = """
                    INSERT INTO fact_sales 
                    (order_number, product_key, customer_key, order_date, shipping_date,
                    due_date, sales_amount, quantity, price)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
            
            # Execute batch insert
            cursor.executemany(query, [row for row in csv_reader])
            conn.commit()

    # Load data into tables
    load_csv_to_table('dim_customers', 
                     'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/gold.dim_customers.csv')
    load_csv_to_table('dim_products', 
                     'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/gold.dim_products.csv')
    load_csv_to_table('fact_sales', 
                     'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/gold.fact_sales.csv')

    print("Database and tables created successfully, data loaded.")

except Error as e:
    print(f"Error: {e}")

finally:
    if conn.is_connected():
        cursor.close()
        conn.close()
        print("MySQL connection closed.")


# In[ ]:





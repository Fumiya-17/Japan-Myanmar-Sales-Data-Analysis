import pandas as pd
import sqlite3
import os

def load_csv(csv_path, sqlite_path, table_name):
    """
    write a script that loads source csv data to sqllite file in the staging area 
    """

    try: 
        df = pd.read_csv(csv_path)
        conn = sqlite3.connect(sqlite_path)
        df.to_sql(table_name, conn, if_exists='replace', index=False)

        print(f"Data loaded successfully into {table_name} table in {sqlite_path}")
    
    except Exception as e:
        print(f"An error occurred or data already exists: {e}")
    
    finally:
        if 'conn' in locals():
            conn.close()

    

# ------------
# Japan Staging Area Extraction
# ------------

def load_japan_staging_area():
    Japan_csv_paths = [
        "G:\Schoolstuff\ADMS\etl-activity-main\data\source\japan_store\japan_branch.csv",
        "G:\Schoolstuff\ADMS\etl-activity-main\data\source\japan_store\japan_Customers.csv",
        "G:\Schoolstuff\ADMS\etl-activity-main\data\source\japan_store\japan_items.csv",
        "G:\Schoolstuff\ADMS\etl-activity-main\data\source\japan_store\japan_payment.csv",
        "G:\Schoolstuff\ADMS\etl-activity-main\data\source\japan_store\sales_data.csv"
    ]

    Japan_sqlite_path = "G:\Schoolstuff\ADMS\etl-activity-main\data\Staging\japan staging area.db"

    Japan_table_names = [
        "japan_branch",
        "japan_Customers",
        "japan_items",
        "japan_payment",
        "japan_sales_data" 
    ] 

    for csv_path, table_name in zip(Japan_csv_paths, Japan_table_names):
        load_csv(csv_path, Japan_sqlite_path, table_name)

    print("Japan staging area data loaded successfully.")


# ------------
# Myanmar Staging Area Extraction
# ------------


def load_myanmar_staging_area():
    Myanmar_csv_paths = [
        "G:\Schoolstuff\ADMS\etl-activity-main\data\source\myanmar_store\myanmar_branch.csv",
        "G:\Schoolstuff\ADMS\etl-activity-main\data\source\myanmar_store\myanmar_customers.csv",
        "G:\Schoolstuff\ADMS\etl-activity-main\data\source\myanmar_store\myanmar_items.csv",
        "G:\Schoolstuff\ADMS\etl-activity-main\data\source\myanmar_store\myanmar_payment.csv",
        "G:\Schoolstuff\ADMS\etl-activity-main\data\source\myanmar_store\sales_data.csv"
    ]

    Myanmar_sqlite_path = "G:\Schoolstuff\ADMS\etl-activity-main\data\Staging\myanmar staging area.db"

    Myanmar_table_names = [
        "myanmar_branch",
        "myanmar_customers",
        "myanmar_items",
        "myanmar_payment",
        "myanmar_sales_data" 
    ] 

    for csv_path, table_name in zip(Myanmar_csv_paths, Myanmar_table_names):
        load_csv(csv_path, Myanmar_sqlite_path, table_name)

    print("Myanmar staging area data loaded successfully.")


# Execute the loading functions
load_japan_staging_area()
load_myanmar_staging_area()


# -------------
# extra stuff
# -------------


def drop_table(sqlite_path, table_name):
    """
    Drops a table from the specified SQLite database.
    """
    try:
        conn = sqlite3.connect(sqlite_path)
        cursor = conn.cursor()
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        conn.commit()
        print(f"Table {table_name} dropped successfully from {sqlite_path}.")
    except Exception as e:
        print(f"An error occurred while dropping the table: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

drop_table("G:\Schoolstuff\ADMS\etl-activity-main\data\Staging\myanmar staging area.db", "sales_data")
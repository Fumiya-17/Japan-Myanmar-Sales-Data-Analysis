import sqlite3
import pandas as pd

def clean_sqlite_table():

    """
    read from staging and perform data cleaning
    Standardize values across datasets (e.g., Japan store item prices in JPY and Myanmar store item prices in USD are converted to a common currency or format).
    """
    # Connect to SQLite
    # Read table into DataFrame
    # ------------------------------
    # EXAMPLE DATA CLEANING STEPS
    # ------------------------------
    # 1. Strip extra whitespace from column names
    # 2. Strip whitespace inside text columns
    # 3. Replace empty strings with NaN
    # 4. Drop duplicate rows
    # ------------------------------
    # SAVE CLEANED DATA
    # ------------------------------


# ==============
# Renaming Id columns for future merging
# ==============

jap_path = r"G:\Schoolstuff\ADMS\etl-activity-main\data\Staging\japan staging area.db"
myn_path = r"G:\Schoolstuff\ADMS\etl-activity-main\data\Staging\myanmar staging area.db"


japan_table_names = [
        "japan_branch",
        "japan_Customers",
        "japan_items",
        "japan_payment",
    ]

myanmar_table_names = [
        "myanmar_branch",
        "myanmar_customers",
        "myanmar_items",
        "myanmar_payment",
    ]


def rename_column(db_path, table_name, old_name, new_name):

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(f"ALTER TABLE {table_name} RENAME COLUMN {old_name} TO {new_name}")
    conn.commit()
    conn.close()

def rename_table(db_path, old_table_name, new_table_name):

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(f"ALTER TABLE {old_table_name} RENAME TO {new_table_name}")
    conn.commit()
    conn.close()

def rename_japan_customer_col():
    try:
        rename_table(jap_path, "japan_customerssss", "japan_customers")
    except:
        pass
# rename_japan_customer_col

def rename_japan_id_cols():
    
    db_path = r"G:\Schoolstuff\ADMS\etl-activity-main\data\Staging\japan staging area.db"
    conn = sqlite3.connect(db_path)

    japan_new_names = [
        "branch_id",
        "customer_id",
        "product_id",
        "payment_id",
    ]   


    for table_name, new_name in zip(japan_table_names,japan_new_names):
        try:
            rename_column(db_path, table_name, "id", new_name)
        except Exception as e:
            if 'no such column' in str(e):
                print("no such column")
                pass
            else:
                print(f"An error occurred during renaming: {e}")
        finally:
                conn.close()
                
def rename_myanmar_id_cols():
    
    db_path = r"G:\Schoolstuff\ADMS\etl-activity-main\data\Staging\myanmar staging area.db"
    conn = sqlite3.connect(db_path)

    myanmar_new_names = [
        "branch_id",
        "customer_id",
        "product_id",
        "payment_id",
    ]   

    for table_name, new_name in zip(myanmar_table_names,myanmar_new_names):
        try:
            rename_column(db_path, table_name, "id", new_name)

        except Exception as e:
            if 'no such column' in str(e):
                print("no such column")
                pass
            else:
                print(f"An error occurred during renaming: {e}")
        finally:
            conn.close()

# rename_japan_id_cols()
# rename_myanmar_id_cols()


# ==============
# Rename Sales data columns
# ==============

def rename_sales_columns(db_path, table_name):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    sales_rename_mappings = {
        "'invoice_id'": "invoice_id",
        "'branch_id'": "branch_id",
        "'customer_id'": "customer_id",
        "'product_id'": "product_id",
        "'quantity'": "quantity",
        "'date'": "date",
        "'time'": "time",
        "'payment'": "payment_id",
        "'rating'": "rating"
    }

    for old_name, new_name in sales_rename_mappings.items():
            try:

                cursor.execute(f'ALTER TABLE "{table_name}" RENAME COLUMN "{old_name}" TO "{new_name}"')
                print(f"Fixed: {old_name} -> {new_name}")
                
            except Exception as e:
                if 'no such column' in str(e):
                    pass 
                else:
                    print(f"Error renaming {old_name}: {e}")
    
    
    conn.commit()




# rename_sales_columns(jap_path, "japan_sales_data")
# rename_sales_columns(myn_path, "myanmar_sales_data")


# =========
# Other column renaming
# =========

def rename_name_cols(db_path, table):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    new_name_names = [
        "branch_name",
        "customer_name",
        "product_name",
        "payment_name"
    ]

    for table_name, new_name in zip(table, new_name_names):
        try:
           rename_column(db_path, table_name, "name", new_name)
        except Exception as e:
            pass

#rename_name_cols(jap_path, japan_table_names)
#rename_name_cols(myn_path, myanmar_table_names)
#rename_column(myn_path, "myanmar_items", "payment_name", "product_name")
#rename_column(myn_path, "myanmar_items", "type", "category")
#rename_column(myn_path, "myanmar_customers", "type", "membership")

# JPY TO USD
def jpy_to_usd():
    conn = sqlite3.connect(jap_path)
    cursor = conn.cursor()

    cursor.execute("""
    UPDATE JM_Sales_Data
    SET price = ROUND(price * 0.0065, 2)
                   
""")
    conn.commit()
    conn.close()

#jpy_to_usd()




























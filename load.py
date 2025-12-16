import pandas as pd
import sqlite3
import os


jap_path = r"G:\Schoolstuff\ADMS\etl-activity-main\data\Staging\japan staging area.db"
myn_path = r"G:\Schoolstuff\ADMS\etl-activity-main\data\Staging\myanmar staging area.db"



# ==============
# Merging tables and coverting consolidated tables to dataframes
# ==============




def japan_merge_tables(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()


    try:
        cursor.execute("DROP TABLE IF EXISTS Japan_Merged")
        cursor.execute("""
        CREATE TABLE Japan_Merged AS
        SELECT japan_branch.branch_name, japan_branch.city, japan_customers.customer_name, japan_customers.membership, 
                       japan_customers.gender, japan_items.product_name, japan_items.category, japan_items.price, japan_payment.payment_name,
                       japan_sales_data.quantity, japan_sales_data.date, japan_sales_data.time, japan_sales_data.rating
        FROM japan_sales_data
        LEFT JOIN japan_customers ON japan_sales_data.customer_id = japan_customers.customer_id
        LEFT JOIN japan_branch ON japan_sales_data.branch_id = japan_branch.branch_id
        LEFT JOIN japan_items ON japan_sales_data.product_id = japan_items.product_id
        LEFT JOIN japan_payment ON japan_sales_data.payment_id = japan_payment.payment_id      
        """)
        conn.commit()
    except Exception as e:
        if 'already exists' in str(e):
            print("Table already exists.")
        else:
            print(f"Error creating table: {e}")
    finally:
        df_japan = pd.read_sql("SELECT * FROM Japan_Merged", conn) 
        conn.close()  
    return df_japan
   
def myanmar_merge_tables(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()


    try:
        cursor.execute("DROP TABLE IF EXISTS Myanmar_Merged")
        cursor.execute("""
        CREATE TABLE Myanmar_Merged AS
        SELECT 
            myanmar_branch.branch_name, 
            myanmar_branch.city, 
            myanmar_customers.customer_name, 
            myanmar_customers.membership, 
            myanmar_customers.gender, 
            myanmar_items.product_name, 
            myanmar_items.category, 
            myanmar_items.price, 
            myanmar_payment.payment_name,
            myanmar_sales_data.quantity, 
            myanmar_sales_data.date, 
            myanmar_sales_data.time, 
            myanmar_sales_data.rating
        FROM myanmar_sales_data
        LEFT JOIN myanmar_customers ON myanmar_sales_data.customer_id = myanmar_customers.customer_id
        LEFT JOIN myanmar_branch ON myanmar_sales_data.branch_id = myanmar_branch.branch_id
        LEFT JOIN myanmar_items ON myanmar_sales_data.product_id = myanmar_items.product_id
        LEFT JOIN myanmar_payment ON myanmar_sales_data.payment_id = myanmar_payment.payment_id      
        """)
        conn.commit()
    except Exception as e:
        if 'already exists' in str(e):
            print("Table already exists.")
        else:
            print(f"Error creating table: {e}")
    finally:
        df_myanmar = pd.read_sql("SELECT * FROM Myanmar_Merged", conn)
        conn.close()
    return df_myanmar

# Personal peek function for debugging

def peek():
    conn = sqlite3.connect(jap_path)
    cursor = conn.cursor()

    cursor.execute("""
    SELECT * FROM japan_items
                   
""")
    rows = cursor.fetchall()
    
    for row in rows:
        print(row)

    conn.close()
    
peek()


def create_master_union(japan_db_path, myanmar_db_path):
    conn = sqlite3.connect(japan_db_path)
    cursor = conn.cursor()

    try:
    
        cursor.execute("ATTACH DATABASE ? AS db2", (myanmar_db_path,))
        
        cursor.execute("DROP TABLE IF EXISTS Global_Sales_Data")
        
        cursor.execute("""
        CREATE TABLE JM_Sales_Data AS
        
        SELECT *, 'Japan' as country FROM Japan_Merged
        
        UNION ALL
        
        SELECT *, 'Myanmar' as country FROM db2.Myanmar_Merged
        """)
        
        conn.commit()
        
    except Exception as e:
        print(f"SQL Union Error: {e}")
        
    finally:
        df_final = pd.read_sql("SELECT * FROM JM_Sales_Data", conn)
        cursor.execute("DETACH DATABASE db2")
        conn.close()
        
    return df_final

df_combined = create_master_union(jap_path, myn_path)

df_combined = df_combined.drop_duplicates()

print(df_combined)


df_combined["date"] = pd.to_datetime(df_combined['date'])

df_combined['time'] = pd.to_datetime(df_combined['time'], format='%H:%M:%S').dt.time

df_combined.to_csv(r"RATUNIL JM sales data.csv", index=False)


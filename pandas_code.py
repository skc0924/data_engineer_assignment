import sqlite3
import pandas as pd

def pandas_solution(db_file_path,output_file_path):
    try:
        connection = sqlite3.connect(db_file_path)

        customers_df = pd.read_sql_query("SELECT * FROM customers", connection)
        sales_df = pd.read_sql_query("SELECT * FROM sales", connection)
        orders_df = pd.read_sql_query("SELECT * FROM orders", connection)
        items_df = pd.read_sql_query("SELECT * FROM items", connection)
        customers_filtered = customers_df[(customers_df['age'] >= 18) & (customers_df['age'] <= 35)]

        merged_df = pd.merge(customers_filtered, sales_df, on='customer_id')
        merged_df = pd.merge(merged_df, orders_df, on='sales_id')
        merged_df = pd.merge(merged_df, items_df, on='item_id')

        result_df = merged_df.groupby(['customer_id', 'age', 'item_name']).agg({'quantity': 'sum'}).reset_index()
        result_df = result_df[result_df['quantity'] > 0]

        result_df['quantity'] = result_df['quantity'].astype(int)
        new_column_names = {'customer_id': 'Customer','age': 'Age','item_name': 'Item','quantity':'Quantity'}

        result_df.rename(columns=new_column_names, inplace=True)
        result_df.to_csv(output_file_path,sep=';',index=False)

        connection.close()

        return result_df
    except Exception as e:
        print("Error:", e)
        return None


db_file_path="C:\\Users\\Admin\\Downloads\\Data Engineer_ETL Assignment.db"
output_file_path="C:\\Users\\Admin\\Downloads\\output_file.csv"
print(pandas_solution(db_file_path,output_file_path))
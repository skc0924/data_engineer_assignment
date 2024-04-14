import sqlite3

def sql_solution(db_file_path):
    try: 
        connection = sqlite3.connect(db_file_path)
        cursor = connection.cursor()
        cursor.execute('''SELECT c.customer_id AS Customer, c.age AS Age, i.item_name AS Item, SUM(o.quantity) AS Quantity
            FROM customers c
            JOIN sales s ON c.customer_id = s.customer_id
            JOIN orders o ON s.sales_id = o.sales_id
            JOIN items i ON o.item_id = i.item_id
            WHERE c.age BETWEEN 18 AND 35
            GROUP BY c.customer_id, i.item_id
            HAVING SUM(o.quantity) > 0
            ORDER BY c.customer_id, i.item_id
        ''')
        rows = cursor.fetchall()
        return rows
        
    except Exception as e:
        print(e)
        
    finally:
        cursor.close()
        connection.close()

    
    return df

db_file_path="C:\\Users\\Admin\\Downloads\\Data Engineer_ETL Assignment.db"
print(sql_solution(db_file_path))
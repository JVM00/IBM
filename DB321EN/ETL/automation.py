# Import libraries required for connecting to mysql

# Import libraries required for connecting to DB2 or PostgreSql

# Connect to MySQL
import mysql.connector

# Connect to DB2 or PostgreSql


import psycopg2


# Find out the last rowid from DB2 data warehouse or PostgreSql data warehouse
# The function get_last_rowid must return the last rowid of the table sales_data on the IBM DB2 database or PostgreSql.

def get_last_rowid():

    conn = psycopg2.connect(database="postgres", user="postgres", password="XXXX", host="127.0.0.1", port="5432")
    cursor = conn.cursor()

    # Assuming you have a table named 'your_table_name' with a serial or primary key column named 'id'
    #sql_string = "INSERT INTO your_table_name (column1, column2, ...) VALUES (%s, %s, ...) RETURNING id;"
    sql_string= "SELECT rowid FROM sales_data ORDER BY rowid DESC LIMIT 1"
    cursor.execute(sql_string)  # Replace with actual values
    last_inserted_id = cursor.fetchall()
    last_inserted_id = list(last_inserted_id[0])
    #print (last_inserted_id)
    conn.close()
    return last_inserted_id

last_row_id = get_last_rowid()
print("Last row id on production datawarehouse = ", last_row_id)

# List out all records in MySQL database with rowid greater than the one on the Data warehouse
# The function get_latest_records must return a list of all records that have a rowid greater than the last_row_id in the sales_data table in the sales database on the MySQL staging data warehouse.

def get_latest_records(rowid):
    connection = mysql.connector.connect(user='admin', password='XXXX',host='localhost',database='sales')
    cursor = connection.cursor()
    #SQL = ("SELECT rowid FROM data_sales WHERE rowid > %s )",rowid)
    SQL = """SELECT * FROM sales_data WHERE rowid > %s """
    
    last_row=rowid
    #last_row=[7000]
    if last_row== []:
        last_row= [0]
    
    cursor.execute(SQL,last_row)
   # connection.commit()
    latest_records = cursor.fetchall()
    connection.close()
    return latest_records

new_records = get_latest_records(last_row_id)

print("New rows on staging datawarehouse = ", len(new_records))

# Insert the additional records from MySQL into DB2 or PostgreSql data warehouse.
# The function insert_records must insert all the records passed to it into the sales_data table in IBM DB2 database or PostgreSql.

def insert_records(records):
    conn = psycopg2.connect(database="postgres", user="postgres", password="XXX", host="127.0.0.1", port="5432")
    cursor = conn.cursor()

    if records != []: 
   	 for row in records:
    		#cursor.execute("INSERT INTO sales_data (rowid,product_id,customer_id,price,quantity,timestamp) VALUES (%s, %s, %s,%s, %s, %s)", 
    		#		(row[0], row[1], row[2],row[3],row[4],row[5])) 
      		cursor.execute("INSERT INTO public.sales_data (rowid, product_id, customer_id, quantity) VALUES (%s, %s, %s,%s);", (row[0], row[1], row[2],row[3]))
      		#print((row[0], row[1], row[2],row[3]))
     	 	
      		   		
    cursor.execute("SELECT MAX(rowid) FROM sales_data;")  
    max_rowid= cursor.fetchall()
    max_rowid= list(max_rowid[0])
    print(max_rowid)	 	
    conn.commit()
    conn.close()
    
insert_records(new_records)

print("New rows inserted into production datawarehouse = ", len(new_records))

# disconnect from mysql warehouse

# disconnect from DB2 or PostgreSql data warehouse 

# End of program

import mysql.connector
from resource.dev import config

# database connection parameters
config = {
    "user": config.sql_properties["user"],
    "password": config.sql_properties["password"],
    "host": config.sql_properties["host"],
    "database": config.database_name,
}

# Create a connection to the MySQL database
try:
    connection = mysql.connector.connect(**config)

    if connection.is_connected():
        print("Connected to MySQL database")

    cursor = connection.cursor()
    cursor.execute("select * from product_staging_table")
    data = cursor.fetchall()
    print(data)

except mysql.connector.Error as e:
    print(f"Error: {e}")

finally:
    # Close the database connection when done
    if 'connection' in locals():
        connection.close()
        print("Connection closed")
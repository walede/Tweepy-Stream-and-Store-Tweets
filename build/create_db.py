import mysql.connector
from mysql.connector import errorcode

def create_database(cursor, db_name):
    """Function creates a MySQL database.
       ----------------------------------
       arg1:MySQL cursor
       arg2: database name
    """
    try:
        cursor.execute(f"CREATE DATABASE {db_name}")
        print(f"Created database: {db_name}.")
    except mysql.connector.Error as err:
        if err.errno == 1007:
            print(f"Database '{db_name}' already exists.")
            pass
        else:
            print(f"Failed creating database: {db_name}.")
            exit(1)

def use_database(cursor, db_name):
    """Function uses a database with db_name.
       ----------------------------------
       arg1:MySQL cursor
       arg2: database name
    """
    try:
        cursor.execute(f"USE {db_name}")
        print(f"Using database: {db_name}")
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_BAD_DB_ERROR:
            print(f"Database {db_name} does not exist.")
            create_database(cursor, db_name)
            print(f"Database {db_name} is now created successfully.")
            use_database(cursor, db_name)
        else:
            print(err)
            exit(1)

def drop_database(cursor, db_name):
    """Function drops a MySQL database.
       ----------------------------------
       arg1:MySQL cursor
       arg2: database name
    """
    try:
        cursor.execute(f"DROP DATABASE {db_name}")
        print(f"Dropped database '{db_name}'.")
    except mysql.connector.Error as err:
        if(err.errno == 1008):
            print(f'Database "{db_name}" does not exist, moving on...')
            pass
        else:
            print(f"Error {err.errno}: Failed dropping database: {db_name}\n{err.msg}")
            exit(1)

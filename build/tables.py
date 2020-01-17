import mysql.connector
from mysql.connector import errorcode
import pandas as pd
import re

def create_table(cursor, TABLES):
    """This function creates the tables passed in. 
    ---------------------------------------------
    arg1: Mysql_connectior cursor 
    arg2:Tables in a dictionary
    """
    for t_name,t_desc in TABLES.items():
        try:
            print(f"Creating table '{t_name}'...")
            cursor.execute(t_desc)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                print(f"{t_name} already exists, moving on...")
                pass
            else:
                print(err.msg+".")
                exit(1)
        else:
            print("OK")
    return None

def drop_table(cursor, t_name):
    """This function drops the table with the name
       passed in.
        ----------------------------
        arg1: MySQL_connector cursor
        arg2: the table name
    """    
    try:
        print(f"Dropping table '{t_name}'...")
        cursor.execute(f'DROP TABLE {t_name}')
    except mysql.connector.Error as err:
        if err.errno == 1051:
            print(f"Table '{t_name}' DNE, moving on...")
            pass
        else:
            print(str(err.errno) + ": " + err.msg + ".")
            exit(1)
    else:
        print("OK")
    return None

def bulk_drop_table(cursor, t_name_list):
    """This function drops the tables in the list.
        ----------------------------
        arg1: MySQL_connector cursor
        arg2: the table names in a list
    """
    for t_name in t_name_list:
        try:
            print(f"Dropping table '{t_name}'...")
            cursor.execute(f'DROP TABLE {t_name}')
        except mysql.connector.Error as err:
            if err.errno == 1051:
                print(f"Table '{t_name}' DNE, moving on...")
                pass
            else:
                print(str(err.errno) + ": " + err.msg+".")
                exit(1)
        else:
            print("OK")
    return None
   
def insert_into_table(cursor, t_name, insert, 
                      cnx, autocommit=True):
    """This function inserts into the tables with 't_name'.
        ----------------------------
        arg1: MySQL_connector cursor
        arg2: the table name
        arg3: The insert statement
        arg4: the connection object
        arg5: does not commit the transaction if false, defaults true
    """ 
    try:
        cursor.execute(insert)
        if autocommit:
            if not cnx.autocommit:
                cnx.commit()
        result = cursor
        print(f"Number of rows affected by statement '{result.statement}':"
              f"{result.rowcount}.")
        print("Warnings: " + str(result.fetchwarnings()) + ".")
    except mysql.connector.Error as err:
        print(err.msg + ".")
    else:
        print("Done Inserting.")

def bulk_insert_into_table(cursor, operation, cnx, 
                mute=True, autocommit=True):
    """This function inserts the statements in the string
        delimited by ';'.
        ----------------------------
        arg1: MySQL_connector cursor
        arg2: the operations passed in a string
        arg3: The connection object
        arg4: Prints out every statement if false, defaults true
        arg5: does not commit the transaction if false, defaults true
    """                
    op_list=re.split(';\s*',operation)
    count=0
    for op in op_list:
        if mute:
            try:
                cursor.execute(op)
                if autocommit:
                    if not cnx.autocommit:
                        cnx.commit()
                result = cursor
                count += result.rowcount
                print("Warnings: " + str(result.fetchwarnings()) + ".")
            except mysql.connector.Error as err:
                print(err.msg + ".")
        else:
            try:
                cursor.execute(op)
                if autocommit:
                    if not cnx.autocommit:
                        cnx.commit()
                result = cursor
                print(f"Number of rows affected by statement "
                      f"'{result.statement}': {result.rowcount}.")
                print("Warnings: " + str(result.fetchwarnings()) + ".")
            except mysql.connector.Error as err:
                print(err.msg + ".")
    if mute:
        print(f"Number of rows affected by statement: {count}.")
        print("Done Inserting.")

def query_table(cursor, t_name, query, cnx):
    """This function queries the table and prints
       out the dataframe and returns it as an object.
       ---------------------------------------------
       arg1: MySQL_connector cursor
       arg2: table name
       arg3: query statement
       arg4: connection object
    """
    try:
        df=pd.read_sql(query, cnx)
        print(df)
        return df
    except mysql.connector.Error as err:
        if err.errno == 1051:
            print(f"Cant read '{t_name}', table D.N.E, moving on...")
            pass
        else:
            print(err.msg + ".")
            exit(1)
    return None

def del_from_table(cursor, t_name, del_op, 
                      cnx, autocommit=True):
    """This function deletes from the tables with 't_name'.
        ----------------------------
        arg1: MySQL_connector cursor
        arg2: the table name
        arg3: The delete statement
        arg4: the connection object
        arg5: does not commit the transaction if false, defaults true
    """ 
    try:
        cursor.execute(del_op)
        if autocommit:
            if not cnx.autocommit:
                cnx.commit()
        result = cursor
        print(f"Number of rows affected by statement '{result.statement}':"
              f"{result.rowcount}.")
        print("Warnings: " + str(result.fetchwarnings()) + ".")
    except mysql.connector.Error as err:
        print(err.msg + ".")
    else:
        print("Done Deleting.")

def bulk_del_from_table(cursor, operation, cnx, 
                mute=True, autocommit=True):
    """This function deletes from the statements in the string
        delimited by ';'.
        ----------------------------
        arg1: MySQL_connector cursor
        arg2: the operations passed in a string
        arg3: The connection object
        arg4: Prints out every statement if false, defaults true
        arg5: does not commit the transaction if false, defaults true
    """                
    op_list=re.split(';\s*',operation)
    count=0    
    for op in op_list:
        if mute:
            try:
                cursor.execute(op)
                if autocommit:
                    if not cnx.autocommit:
                        cnx.commit()
                result = cursor
                count += result.rowcount
                print("Warnings: " + str(result.fetchwarnings()) + ".")
            except mysql.connector.Error as err:
                print(err.msg + ".")
        else:
            try:
                cursor.execute(op)
                if autocommit:
                    if not cnx.autocommit:
                        cnx.commit()
                result = cursor
                print(f"Number of rows affected by statement '"
                      f"{result.statement}': {result.rowcount}.")
                print("Warnings: " + str(result.fetchwarnings()) + ".")
            except mysql.connector.Error as err:
                print(err.msg + ".")
    if mute:
        print(f"Number of rows affected by statement: {count}.")
        print("Done Deleting.")

def update_table(cursor, t_name, update_op, 
                      cnx, autocommit=True):
    """This function updates from the tables with 't_name'.
        ----------------------------
        arg1: MySQL_connector cursor
        arg2: the table name
        arg3: The update statement
        arg4: the connection object
        arg5: does not commit the transaction if false, defaults true
    """ 
    try:
        cursor.execute(update_op)
        if autocommit:
            if not cnx.autocommit:
                cnx.commit()
        result = cursor
        print(f"Number of rows affected by statement '{result.statement}':"
              f"{result.rowcount}.")
        print("Warnings: " + str(result.fetchwarnings()) + ".")
    except mysql.connector.Error as err:
        print(err.msg + ".")
    else:
        print("Done Updating.")

def bulk_update_from_table(cursor, operation, cnx, 
                mute=True, autocommit=True):
    """This function updates from the statements in the string
        delimited by ';'.
        ----------------------------
        arg1: MySQL_connector cursor
        arg2: the operations passed in a string
        arg3: The connection object
        arg4: Prints out every statement if false, defaults true
        arg5: does not commit the transaction if false, defaults true
    """                
    op_list=re.split(';\s*',operation)
    count=0
    for op in op_list:
        if mute:
            try:
                cursor.execute(op)
                if autocommit:
                    if not cnx.autocommit:
                        cnx.commit()
                result = cursor
                count += result.rowcount
                print("Warnings: " + str(result.fetchwarnings()) + ".")
            except mysql.connector.Error as err:
                print(err.msg + ".")
        else:
            try:
                cursor.execute(op)
                if autocommit:
                    if not cnx.autocommit:
                        cnx.commit()
                result = cursor
                print(f"Number of rows affected by statement '"
                      f"{result.statement}': {result.rowcount}.")
                print("Warnings: " + str(result.fetchwarnings()) + ".")
            except mysql.connector.Error as err:
                print(err.msg + ".")
    if mute:
        print(f"Number of rows affected by statement: {count}.")
        print("Done Updating.")

def CUD_table(cursor,t_name,query,cnx,autocommit=True):
    """This function can insert, update, or delete from 't_name'.
        ----------------------------
        arg1: MySQL_connector cursor
        arg2: the table name
        arg3: the query statement
        arg4: The connection object
        arg5: does not commit the transaction if false, defaults true
    """     
    try:
        cursor.execute(query)
        result=cursor
        if result.with_rows:
            print(f"Cant issue: {result.statement}.")
            print("Please pass an Insert, Update or Delete Statement.")
            print("This statement will not be committed.")
            exit(1)
        else:
            try:
                print(rf"Number of rows affected by statement"
                        rf"'{result.statement}': {result.rowcount}.")
            except WindowsError as we:
                print(f"Number of rows affected by statement: {result.rowcount}.")
        if autocommit:
            if not cnx.autocommit:
                cnx.commit()
        print("Warnings: " + str(result.fetchwarnings()) + ".")
    except mysql.connector.Error as err:
        if err.errno==1062:
            print('Already in table, w/ Primary key, will not insert.')
            pass
        elif err.errno==1051:
            print(f'Cant delete "{t_name}", table D.N.E, moving on...')
            pass
        elif err.errno==1406:
            print(f'Data too long for the column, will not be inserted')
            pass
        elif err.errno==1064:
            print(err.msg + ".")
            exit(1)
        else:
            print(err.errno + ": " + err.msg + ".")
            exit(1)

def bulk_CUD_table(cursor, operation, cnx, 
                mute=True, autocommit=True):
    """This function contains combinations of insert, 
       delete and update.
        --------------------------------------------
        arg1: MySQL_connector cursor
        arg2: the operations passed in a string
        arg3: The connection object
        arg4: Prints out every statement if false, defaults true
        arg5: does not commit the transaction if false, defaults true
    """                
    op_list=re.split(';\s*',operation)
    count=0
    for op in op_list:
        if mute:
            try:
                cursor.execute(op)
                if autocommit:
                    if not cnx.autocommit:
                        cnx.commit()
                result = cursor
                count += result.rowcount
                print("Warnings: " + str(result.fetchwarnings()) + ".")
            except mysql.connector.Error as err:
                print(err.msg + ".")
        else:
            try:
                cursor.execute(op)
                if autocommit:
                    if not cnx.autocommit:
                        cnx.commit()
                result = cursor
                print(f"Number of rows affected by statement "
                      f"'{result.statement}': {result.rowcount}.")
                print("Warnings: " + str(result.fetchwarnings()) + ".")
            except mysql.connector.Error as err:
                print(err.msg + ".")
    if mute:
        print(f"Number of rows affected by statement: {count}.")
        print("Done.")

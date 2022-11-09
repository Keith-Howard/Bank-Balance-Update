import pandas as pd
import urllib.parse
from sqlalchemy import create_engine


# make sure pyodbc is installed in project for line 37 to work

# There is no SQL injection because there is no User input being used for query
# input: connection, schema and table name
# processing: reads and prints table
# output: prints out table
def read_sql_table(conn, table, schema):
    df = pd.read_sql_table(table, conn, schema=schema)
    return df


# There is no SQL injection because there is no User input being used for query
# input connection, table data
# processing update balance with transaction table info, use pd.read_sql_query()
# output: updated balance table
def update_account_balance(tran_conn):
    connection = tran_conn.raw_connection()
    cursor = connection.cursor()
    trans_data = pd.read_sql_query('Exec dbo.Bank_Transaction_Data', tran_conn)
    for index, row in trans_data.head(len(trans_data)).iterrows():
        query = 'Exec dbo.Bank_Transactions_Update @Amount = ' + str(row[1]) + ', @Id = ' + str(row[0]) \
                + ', @Tran_type = ' + "'" + row[2] + "'"
        cursor.execute(query)
        return_code = cursor.fetchone()
        if return_code[0] != 'ok':
            print(return_code[0])
    cursor.close()
    connection.commit()
    connection.close()


params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
                                 "SERVER=DESKTOP-IQBNFKO;DATABASE=Keith_Test;"
                                 "Trusted_Connection=yes")
conn = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
print(read_sql_table(conn, 'Bank_Balance', 'dbo'))
print(read_sql_table(conn, 'Bank_Transaction', 'dbo'))
update_account_balance(conn)
print(read_sql_table(conn, 'Bank_Balance', 'dbo'))

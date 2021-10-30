import pyodbc


# input is connection to SQL server and specific table name on database
# processing code, iterate through rows of specific table
# output is the printed rows from specific table
def print_table_rows(conn, table_name):
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM " + table_name)
    for row in cursor:
        print(row)
    cursor.close()


# input is connection to database
# processing, reading tran data table and appending cols ID, Amount and Action to lists in a list
# output is a the list of lists
def trans_table_list(trans_conn):
    list_of_lists = []

    transaction_cursor = trans_conn.cursor()
    transaction_cursor.execute("Exec dbo.Bank_Transaction_Data")

    for row in transaction_cursor:
        cols_list = [row[0], float(row[1]), row[2]]
        list_of_lists.append(cols_list)

    transaction_cursor.close()
    return list_of_lists


# input is connection to SQL server and specific table name on database
# processing is update rows that match ID column in bank_balance with info from Bank_transaction
# output is the updated rows from specific table
def update_table_rows(bal_conn, trans_list):
    for tran_data in trans_list:
        row_id = int(tran_data[0])
        row_amount = float(tran_data[1])
        row_action = str(tran_data[2])
        balance_cursor = bal_conn.cursor()
        balance_cursor.execute('Exec dbo.Bank_Transactions_Update @Amount = ' + str(row_amount) + ', @Id = ' +
                               str(row_id) + ', @Tran_type = ' + row_action)
        balance_cursor.close()
        connection1.commit()  # This saves the data to the database and then unlocks the table


# main program

connection1 = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                             'Server=DESKTOP-IQBNFKO;'
                             'Database=Keith_Test;'
                             'Trusted_Connection=yes')
print('Bank Balances')
print_table_rows(connection1, 'keith_test.dbo.Bank_Balance')
print('Bank Transactions')
print_table_rows(connection1, 'keith_test.dbo.Bank_Transaction')
update_table_rows(connection1, trans_table_list(connection1))
print('New Bank Balances')
print_table_rows(connection1, 'keith_test.dbo.Bank_Balance')

connection1.close()

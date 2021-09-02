import pyodbc
import pandas

def sqlExec(bd,query):
  drive = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=147.135.39.147;DATABASE=%s;UID=SA;PWD=QUi8db9t' % bd
  # print(drive)
  try:
    conn = pyodbc.connect(drive)
    data = pandas.read_sql(query,conn)
    print('Query Ejecutada')
    return data
  except pyodbc.Error as ex:
    sqlstate = ex.args[1]
    return sqlstate

def sqlStore(bd,query):
  drive = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=147.135.39.147;DATABASE=%s;UID=SA;PWD=QUi8db9t' % bd
  # print(drive)
  try:
    conn = pyodbc.connect(drive)
    cursor = conn.cursor()
    cmd_prod_executesp = query
    conn.autocommit = True
    cursor.execute(cmd_prod_executesp)
    conn.close()
    print('Query Ejecutada')
  except pyodbc.Error as ex:
    sqlstate = ex.args[1]
    return sqlstate
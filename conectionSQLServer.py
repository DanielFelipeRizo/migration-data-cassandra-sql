import pyodbc
##pip install pyodbc
conn = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};'
                      'Server=DESKTOP-8CD6IGB;'
                      'Database=migracion_pasantias;'
                      'Username=sa;'
                      'Password=12345;'
                      'Trusted_Connection=yes;')
conn.cursor()


def connLocalSQLServer():
    strConn = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};'
                          'Server=DESKTOP-8CD6IGB;'
                          'Database=migracion_pasantias;'
                          'Username=sa;'
                          'Password=12345;'
                          'Trusted_Connection=yes;')
    return conn.cursor()

# sessionsql = conn.cursor()

# sessionsql.execute('''insert into comercio (celular) VALUES ('523234')''')
# conn.commit()

#ejecutarConsulta = sessionsql.execute(consulta)


# for i in ejecutarConsulta:
#     print(i[0], i[1], i[2], i[3], i[4])
#

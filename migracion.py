from conectionCassandra import conectionCassandra
from conectionSQLServer import connLocalSQLServer


def migrationData():

    #activar sesion conexion cassandra
    sesionCassandra = conectionCassandra().connect()

    consulta = "select id, terminal, transaction_id, estado, forma_pago, authorization_code, channel, date_transaction, debit_amount, " \
               "debit_coin, debit_currency, exchange_rate, exchange_rate_currency, name_account, period, sequential, service_account, " \
               "service_detail, service_name, time_transaction, total_amount, total_amount_currency, id_comercio, id_sucursal, id_terminal, " \
               "nombre_comercio, direccion_sucursal, nombre_encargado, documento_encargado, telefono, nit, id_tipo_transaccion, tipo_transaccion, " \
               "create_by, created_date, updated_by, updated_date, cliente_id from registrar_pago.registrar_pago;"

    ejecutarConsulta = sesionCassandra.execute(consulta)

    dataToList = list(ejecutarConsulta)
    cont = 0

    #sesion sql server
    cursor = connLocalSQLServer()

    #activar identity insert en la tabla para realizar los insert
    actIdentInsert = "SET IDENTITY_INSERT migracion_pasantias.dbo.historico_reportes ON;"
    cursor.execute(actIdentInsert)
    connLocalSQLServer().commit()

    for row in dataToList:
        a = f"INSERT migracion_pasantias.dbo.historico_reportes (id, terminal, transaction_id, estado, forma_pago, authorization_code, channel, " \
            f"date_transaction, debit_amount, debit_coin, debit_currency, exchange_rate, exchange_rate_currency, name_account, period, sequential, " \
            f"service_account, service_detail, service_name, time_transaction, total_amount, total_amount_currency, id_comercio, id_sucursal, " \
            f"id_terminal, nombre_comercio, direccion_sucursal, nombre_encargado, documento_encargado, telefono, nit, id_tipo_transaccion, " \
            f"tipo_transaccion, create_by, created_date, updated_by, updated_date, cliente_id) " \
            f"VALUES ({row[0]}, '{row[1]}', '{row[2]}', '{row[3]}', '{row[4]}', '{row[5]}', '{row[6]}', '{row[7]}', '{row[8]}', '{row[9]}', " \
            f"'{row[10]}', '{row[11]}', '{row[12]}', '{row[13]}', '{row[14]}', '{row[15]}', '{row[16]}', '{row[17]}', '{row[18]}', '{row[19]}'," \
            f" '{row[20]}', '{row[21]}', '{row[22]}', '{row[23]}', '{row[24]}', '{row[25]}', '{row[26]}', '{row[27]}', '{row[28]}', '{row[29]}'," \
            f" '{row[30]}', '{row[31]}', '{row[32]}', '{row[33]}', '{row[34]}', '{row[35]}', '{row[36]}', '{row[37]}')"

        # activar conexion sql
        cursor.execute(a)
        connLocalSQLServer().commit()

        cont = cont+1
        print(cont, " insercion exitosa")

        # if cont == 10:
        #     break

    #desactivar identity insert en la tabla para realizar los insert
    desactIdentInsert = "SET IDENTITY_INSERT migracion_pasantias.dbo.historico_reportes OFF;"
    cursor.execute(desactIdentInsert)
    connLocalSQLServer().commit()

    connLocalSQLServer().close()


try:
    migrationData()

except Exception as e:
    print("error en la conexion o insersion. ", e)

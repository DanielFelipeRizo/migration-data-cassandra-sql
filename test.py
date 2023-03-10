#pruebas unitarias del script

from conectionCassandra import conectionCassandra
from conectionSQLServer import connLocalSQLServer


def migrationData():

    #activar sesion conexion cassandra
    sesionCassandra = conectionCassandra().connect()

    consulta = "select id, terminal, transaction_id, estado from registrar_pago.registrar_pago;"

    ejecutarConsulta = sesionCassandra.execute(consulta)

    dataToList = list(ejecutarConsulta)
    cont = 0

    #sesion sql server
    cursor = connLocalSQLServer()


    actIdentInsert = "SET IDENTITY_INSERT migracion_pasantias.dbo.historico_reportes ON;"
    cursor.execute(actIdentInsert)
    connLocalSQLServer().commit()

    for row in dataToList:
        a = f"INSERT migracion_pasantias.dbo.historico_reportes (id, terminal, transaction_id, estado) VALUES ({row[0]}, '{row[1]}', '{row[2]}', '{row[3]}')"

        # activar conexion sql
        cursor.execute(a)
        connLocalSQLServer().commit()

        cont = cont+1
        print(cont, " insercion exitosa")

    desactIdentInsert = "SET IDENTITY_INSERT migracion_pasantias.dbo.historico_reportes OFF;"
    cursor.execute(desactIdentInsert)
    connLocalSQLServer().commit()

    connLocalSQLServer().close()


try:
    migrationData()

except Exception as e:
    print("error en la conexion o insersion. ", e)

import conectionSQLServer
from conectionCassandra import conexioncassandra

try:

    #activar conexion cassandra
    sessioncassandra = conexioncassandra()

    #activar conexion sql
    sessionsql = conectionSQLServer.conn.cursor()

    tablaDb = input("Ingrese el nombre de la tabla a migrar: ")
    columnaTabla = input("Ingrese el nombre de la columna a migrar: ")
    esquemaBD = input("Ingrese el nombre del esquema donde pertenece la tabla: ")


    # tablaDb = "comercio"
    # columnaTabla = "celular"
    # esquemaBD = "comercio"

    consulta = "select " + columnaTabla + " from polaris_cmb_"+esquemaBD+"."+tablaDb

    #print(consulta)
    ejecutarConsulta = sessioncassandra.execute(consulta)

    for i in ejecutarConsulta.all():

        insertEnBdDestino = "insert into " + tablaDb + " (" + columnaTabla + ") values ('"+i[0]+"')"
        sessionsql.execute(insertEnBdDestino)
        conectionSQLServer.conn.commit()
    print('migracion exitosa')

except Exception as e:
    print('error de conexion o insersion')
    
def conexioncassandra():
    ##pip install cassandra-driver
    from cassandra.cluster import Cluster
    from cassandra.auth import PlainTextAuthProvider

    auth_provider = PlainTextAuthProvider(username='polaris_cmb_services', password='07eASGvN9OfMvI3v3glz4SjKle1Vwu')

    cluster = Cluster(['54.83.56.230'], port=9042, auth_provider=auth_provider)

    return cluster.connect()

def conectionCassandra():
    from cassandra.cluster import Cluster
    from cassandra.auth import PlainTextAuthProvider

    auth_provider = PlainTextAuthProvider(username='', password='')

    cluster = Cluster(['127.0.0.1'], port=9042, auth_provider=auth_provider)

    return cluster


# try:
#
#     session = conexioncassandra()
#
#     tablaDb = input("Ingrese el nombre de la tabla a migrar: ");
#     columnaTabla = input("Ingrese el nombre de la columna a migrar: ");
#     esquemaBD = input("Ingrese el nombre del esquema donde pertenece la tabla: ");
#
#     consulta = "select " + columnaTabla + " from polaris_cmb_"+esquemaBD+"."+tablaDb;
#
#     # print(consulta)
#
#     ejecutarConsulta = session.execute(consulta)
#
#     for i in ejecutarConsulta.all():
#
#         print(i[0])
#
#
# except Exception as e:
#     print('error')


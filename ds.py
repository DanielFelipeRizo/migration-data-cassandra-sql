import unittest
from conectionSQLServer import conn as sql_conn
from conectionCassandra import conexioncassandra

def migrar(tablaDb, columnaTabla, esquemaBD, sessioncassandra, sessionsql):
    consulta = "SELECT " + columnaTabla + " FROM polaris_cmb_" + esquemaBD + "." + tablaDb
    ejecutarConsulta = sessioncassandra.execute(consulta)

    for i in ejecutarConsulta.all():
        insertEnBdDestino = "INSERT INTO " + tablaDb + " (" + columnaTabla + ") VALUES ('"+i[0]+"')"
        sessionsql.execute(insertEnBdDestino)
        sessionsql.commit()

class TestMigracion(unittest.TestCase):
    def setUp(self):
        self.sessioncassandra = conexioncassandra()
        self.sessionsql = sql_conn.cursor()

    def test_migrar_exitoso(self):
        tablaDb = "comercio"
        columnaTabla = "celular"
        esquemaBD = "comercio"

        migrar(tablaDb, columnaTabla, esquemaBD, self.sessioncassandra, self.sessionsql)

        consulta = "SELECT COUNT(*) FROM " + tablaDb + " WHERE " + columnaTabla + " IS NOT NULL"
        self.sessionsql.execute(consulta)
        result = self.sessionsql.fetchone()
        self.assertGreater(result[0], 0)

    def test_migrar_falla(self):
        tablaDb = "comercio"
        columnaTabla = "inexistente"
        esquemaBD = "comercio"

        with self.assertRaises(Exception):
            migrar(tablaDb, columnaTabla, esquemaBD, self.sessioncassandra, self.sessionsql)

    def tearDown(self):
        self.sessionsql.close()
        self.sessioncassandra.shutdown()

if __name__ == '__main__':
    unittest.main()

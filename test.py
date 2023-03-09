import unittest
from unittest.mock import patch
import migracion

class TestMigracion(unittest.TestCase):

    def setUp(self):
        self.tablaDb = "comercio"
        self.columnaTabla = "celular"
        self.esquemaBD = "comercio"

    def tearDown(self):
        pass

    @patch('migracion.conexionSQLServer')
    @patch('migracion.conexioncassandra')
    def test_migracion_exitosa(self, mock_sessioncassandra, mock_sessionsql):
        mock_sessioncassandra.execute.return_value.all.return_value = [('1234567890',)]

        migracion.migrar(self.tablaDb, self.columnaTabla, self.esquemaBD)

        mock_sessioncassandra.execute.assert_called_once_with("select celular from polaris_cmb_comercio.comercio")
        mock_sessionsql.execute.assert_called_once_with("insert into comercio (celular) values ('1234567890')")

    @patch('migracion.conexionSQLServer')
    @patch('migracion.conexioncassandra')
    def test_error_de_conexion_o_insersion(self, mock_sessioncassandra, mock_sessionsql):
        mock_sessioncassandra.execute.side_effect = Exception("Error de conexión o consulta")

        result = migracion.migrar(self.tablaDb, self.columnaTabla, self.esquemaBD)

        self.assertEqual(result, "error de conexion o insersion")

if __name__ == '__main__':
    unittest.main()

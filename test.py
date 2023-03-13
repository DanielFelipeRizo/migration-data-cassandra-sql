import unittest
from unittest.mock import patch, MagicMock
from conectionCassandra import conectionCassandra
from conectionSQLServer import connLocalSQLServer
from migracion import migrationData

class TestMigrationData(unittest.TestCase):

    def setUp(self):
        # Crear una base de datos temporal en SQL Server
        self.conn = connLocalSQLServer()
        self.cursor = self.conn.cursor()
        self.cursor.execute('CREATE DATABASE TestDB;')
        self.cursor.execute('USE TestDB;')
        self.cursor.execute('CREATE TABLE historico_reportes (id INT IDENTITY(1,1) PRIMARY KEY, terminal VARCHAR(50), transaction_id VARCHAR(50), estado VARCHAR(50));')
        self.conn.commit()

        # Crear una base de datos temporal en Cassandra
        self.cassandra_session = conectionCassandra().connect()
        self.cassandra_session.execute('CREATE KEYSPACE TestKS WITH REPLICATION = {\'class\': \'SimpleStrategy\', \'replication_factor\' : 1};')
        self.cassandra_session.execute('CREATE TABLE TestKS.registrar_pago (id INT PRIMARY KEY, terminal VARCHAR, transaction_id VARCHAR, estado VARCHAR);')

        # Insertar datos de prueba en Cassandra
        self.cassandra_session.execute('INSERT INTO TestKS.registrar_pago (id, terminal, transaction_id, estado) VALUES (1, \'terminal1\', \'transaction1\', \'estado1\');')
        self.cassandra_session.execute('INSERT INTO TestKS.registrar_pago (id, terminal, transaction_id, estado) VALUES (2, \'terminal2\', \'transaction2\', \'estado2\');')
        self.cassandra_session.execute('INSERT INTO TestKS.registrar_pago (id, terminal, transaction_id, estado) VALUES (3, \'terminal3\', \'transaction3\', \'estado3\');')
        self.cassandra_session.execute('INSERT INTO TestKS.registrar_pago (id, terminal, transaction_id, estado) VALUES (4, \'terminal4\', \'transaction4\', \'estado4\');')
        self.cassandra_session.execute('INSERT INTO TestKS.registrar_pago (id, terminal, transaction_id, estado) VALUES (5, \'terminal5\', \'transaction5\', \'estado5\');')

    def tearDown(self):
        # Eliminar la base de datos temporal en SQL Server
        self.cursor.execute('DROP DATABASE TestDB;')
        self.conn.commit()

        # Eliminar la base de datos temporal en Cassandra
        self.cassandra_session.execute('DROP KEYSPACE TestKS;')

    def test_migrationData(self):
        # Mockear la
        migrationData()

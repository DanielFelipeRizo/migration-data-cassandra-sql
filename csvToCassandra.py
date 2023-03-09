import csv


##pip install cassandra-driver
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

auth_provider = PlainTextAuthProvider(username='', password='')

cluster = Cluster(['127.0.0.1'], port=9042, auth_provider=auth_provider)

session = cluster.connect('registrar_pago')
count = 0
# idError = []

with open(r'C:\Users\WPOSS\Desktop\pasantias\Results.csv', encoding='utf8') as file:
    next(file)
    csvreader = csv.reader(file)

    for row in csvreader:
        # print(row[0])
        a = f"INSERT INTO registrar_pago.registrar_pago (id, terminal, transaction_id, estado, forma_pago, authorization_code, channel, date_transaction, debit_amount, debit_coin, debit_currency, exchange_rate, exchange_rate_currency, name_account, period, sequential, service_account, service_detail, service_name, time_transaction, total_amount, total_amount_currency, id_comercio, id_sucursal, id_terminal, nombre_comercio, direccion_sucursal, nombre_encargado, documento_encargado, telefono, nit, id_tipo_transaccion, tipo_transaccion, create_by, created_date, updated_by, updated_date, cliente_id) VALUES ({row[0]}, '{row[1]}', '{row[2]}', '{row[3]}', '{row[4]}', '{row[5]}', '{row[6]}', '{row[7]}', '{row[8]}', '{row[9]}', '{row[10]}', '{row[11]}', '{row[12]}', '{row[13]}', '{row[14]}', '{row[15]}', '{row[16]}', '{row[17]}', '{row[18]}', '{row[19]}', '{row[20]}', '{row[21]}', '{row[22]}', '{row[23]}', '{row[24]}', '{row[25]}', '{row[26]}', '{row[27]}', '{row[28]}', '{row[29]}', '{row[30]}', 454, '{row[32]}', '{row[33]}', '{row[34]}', '{row[35]}', '{row[36]}', '{row[37]}')"
        session.execute(a)
        count = count + 1
        print("total de inserts: ", count)
        # if count == 1000:
        #     break


# Cerrar la conexión a la base de datos de Cassandra
session.shutdown()
file.close()

        # print(idError)

    # except Exception as e:
    #     print('el registro fallido es: ', row[0], '. Error de conexion o insersion. Exception: ', e)

import csv
import datetime
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

auth_provider = PlainTextAuthProvider(username='', password='')
cluster = Cluster(['127.0.0.1'], port=9042, auth_provider=auth_provider)

session = cluster.connect('registrar_pago')
cont = 0

#prueba cargando un archivo csv mas pequeño

with open(r'C:\Users\WPOSS\Downloads\licenseUsage.csv') as f:
    csvreader = csv.reader(f)
    next(csvreader)  # Saltar la primera fila (cabecera)
    a = list(csvreader)
    for row in a:
        fecha = row[0]
        nombre = row[1]
        ip = row[2]
        os = row[3]

        a = f"INSERT INTO comercio.licencias (dateini, product, ip, os) VALUES ('{fecha}', '{nombre}', '{ip}', '{os}')"
        session.execute(a)
        cont = cont+1
        print(cont,"er insert ejecutado: ", a)
    print("total de inserts: ", cont)

# Cerrar la conexión a la base de datos de Cassandra
session.shutdown()
f.close()

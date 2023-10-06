import openpyxl
import pandas as pd

#archivo_xlsx = 'C:\\Users\\\Public\\Documents\\WPOSS TV.xlsx'
archivo_xlsx = 'C:\\Users\\\Public\\Documents\\6.10 WPOSS TV.xlsx'

# Abre el archivo XLSX
archivo_workbook = openpyxl.load_workbook(archivo_xlsx)

# Selecciona una hoja específica (en este caso, la primera hoja)
hoja = archivo_workbook.active

# Itera a través de las filas de la hoja
for fila in hoja.iter_rows(values_only=True):
    # Accede a cada una de las posiciones (columnas) en cada fila
    asfi = str(fila[0]).strip()  # Primera columna
    id_cor_old = str(fila[1]).strip() # Segunda columna
    tar_v_old = str(fila[2]).strip() # Tercera columna
    id_cor_new = str(fila[3]).strip() # Cuarta columna
    tar_v_new = str(fila[4]).strip() # Cuarta columna

    # actualizar tarjetas virtuales
    stringUpdate = (f"update sucursales set card_accp_merch = '{id_cor_new}', nroCcaBancosol = '{tar_v_new}' "
                    f"where card_accp_merch = '{id_cor_old}' and nroCcaBancosol = '{tar_v_old}';")
    print(stringUpdate)

    # actualizar asfi
    # stringUpdateAsfi = f"update merchants set asfi_comercio = '{fila[0]}' where merchant_id = (select merchant_id from sucursales where card_accp_merch = '{fila[1]}')"
    # print(stringUpdateAsfi)

# Cierra el archivo XLSX después de leerlo
archivo_workbook.close()


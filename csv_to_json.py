import pandas as pd
import os
import glob

# Carpeta base del repositorio (ruta relativa)
carpetas_origen = ["datos_bmv", "datos_us"]
archivo_salida = "stock_data.json"

# Lista para acumular todos los DataFrames
todos_los_datos = []

# Procesar cada carpeta de origen
for carpeta in carpetas_origen:
    archivos_csv = glob.glob(os.path.join(carpeta, "*.csv"))

    for archivo_csv in archivos_csv:
        try:
            df_raw = pd.read_csv(archivo_csv, header=None)

            # Encabezados personalizados
            nuevos_headers = [
                df_raw.iloc[2, 0],  # A3
                df_raw.iloc[0, 1],  # B1
                df_raw.iloc[0, 2],
                df_raw.iloc[0, 3],
                df_raw.iloc[0, 4],
                df_raw.iloc[0, 5],
                df_raw.iloc[0, 6]
            ]

            nombre_ticker = df_raw.iloc[1, 0]  # A2
            valor_ticker = df_raw.iloc[1, 1]   # B2

            # Quitar filas de encabezado
            df = df_raw.iloc[2:].reset_index(drop=True)
            df.columns = nuevos_headers
            df[nombre_ticker] = valor_ticker

            # Quitar fila con encabezado de columna como valor
            df = df[df[nuevos_headers[0]] != nuevos_headers[0]]

            # Convertir fecha a formato BSON
            df[nuevos_headers[0]] = pd.to_datetime(df[nuevos_headers[0]], errors='coerce')
            df[nuevos_headers[0]] = df[nuevos_headers[0]].apply(
                lambda x: {"$date": x.strftime("%Y-%m-%dT%H:%M:%SZ")} if pd.notnull(x) else None
            )

            # Agregar nombre del archivo original como metadato
            nombre_base = os.path.splitext(os.path.basename(archivo_csv))[0]
            df["empresa"] = nombre_base

            # Agregar al conjunto total
            todos_los_datos.append(df)

            print(f"✅ Procesado: {archivo_csv}")

        except Exception as e:
            print(f"❌ Error procesando {archivo_csv}: {e}")

# Unir todos los DataFrames en uno solo
if todos_los_datos:
    df_total = pd.concat(todos_los_datos, ignore_index=True)
    # Crear carpeta de salida si no existe
    os.makedirs("datos_json", exist_ok=True)

    # Guardar en carpeta datos_json
    salida_completa = os.path.join("datos_json", archivo_salida)
    df_total.to_json(salida_completa, orient='records', indent=4)
    print(f"\n✅ Archivo JSON consolidado generado: {archivo_salida}")
else:
    print("\n⚠️ No se encontraron archivos CSV válidos para procesar.")

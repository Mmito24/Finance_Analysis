import pandas as pd
import os
import glob

# Carpeta base del repositorio (ruta relativa)
carpetas_origen = ["datos_bmv", "datos_us"]
carpeta_json = "datos_json"

# Crear carpeta de salida si no existe
os.makedirs(carpeta_json, exist_ok=True)

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

            # Convertir 'Date' a formato BSON compatible con MongoDB
            df[nuevos_headers[0]] = pd.to_datetime(df[nuevos_headers[0]], errors='coerce')
            df[nuevos_headers[0]] = df[nuevos_headers[0]].apply(
                lambda x: {"$date": x.strftime("%Y-%m-%dT%H:%M:%SZ")} if pd.notnull(x) else None
            )

            # Guardar archivo JSON
            nombre_base = os.path.splitext(os.path.basename(archivo_csv))[0]
            archivo_salida = os.path.join(carpeta_json, f"{nombre_base}_mongo.json")
            df.to_json(archivo_salida, orient='records', indent=4)

            print(f"✅ Convertido y guardado: {archivo_salida}")

        except Exception as e:
            print(f"❌ Error procesando {archivo_csv}: {e}")

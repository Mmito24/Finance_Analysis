import os
import pandas as pd
import json

# Carpetas fuente y carpeta destino
carpetas_origen = ["datos_bmv", "datos_us"]
carpeta_destino = "datos_json"
os.makedirs(carpeta_destino, exist_ok=True)

def procesar_archivo(ruta_csv):
    # Leer sin encabezados
    df = pd.read_csv(ruta_csv, header=None)

    # Obtener Ticker desde celda B2 (fila 1, columna 1) [índice base 0]
    ticker = str(df.iloc[1, 1]).strip()

    # Obtener nombre del archivo como Empresa (sin extensión)
    empresa = os.path.splitext(os.path.basename(ruta_csv))[0]

    # Extraer datos reales (desde la fila 2 en adelante)
    data = df.iloc[2:].reset_index(drop=True)

    # Asignar nombres de columnas
    data.columns = ["Date", "Open", "High", "Low", "Close", "Adj Close", "Volume"]

    # Convertir fechas y construir documentos
    registros = []
    for _, row in data.iterrows():
        try:
            fecha_iso = pd.to_datetime(row["Date"]).strftime("%Y-%m-%dT00:00:00Z")
            registro = {
                "Date": {"$date": fecha_iso},
                "Open": str(row["Open"]),
                "High": str(row["High"]),
                "Low": str(row["Low"]),
                "Close": str(row["Close"]),
                "Adj Close": str(row["Adj Close"]),
                "Volume": str(row["Volume"]),
                "Ticker": ticker,
                "Empresa": empresa
            }
            registros.append(registro)
        except Exception as e:
            print(f"Error procesando fila en {ruta_csv}: {e}")
    return registros

# Procesar carpetas
for carpeta in carpetas_origen:
    registros_totales = []
    for archivo in os.listdir(carpeta):
        if archivo.endswith(".csv"):
            ruta = os.path.join(carpeta, archivo)
            print(f"Procesando: {ruta}")
            registros = procesar_archivo(ruta)
            registros_totales.extend(registros)

    # Guardar en archivo JSON por carpeta
    nombre_salida = "bmv_stock_data.json" if carpeta == "datos_bmv" else "us_stock_data.json"
    ruta_salida = os.path.join(carpeta_destino, nombre_salida)
    with open(ruta_salida, "w", encoding="utf-8") as f:
        json.dump(registros_totales, f, indent=4, ensure_ascii=False)
    print(f"Guardado en: {ruta_salida}")

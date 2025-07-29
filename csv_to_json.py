import os
import pandas as pd
import json

# Carpetas fuente y carpeta destino
carpetas_origen = ["datos_bmv", "datos_us"]
carpeta_destino = "datos_json"
os.makedirs(carpeta_destino, exist_ok=True)

def procesar_archivo(ruta_csv):
    # Leer sin encabezado
    df = pd.read_csv(ruta_csv, header=None)

    # Obtener Ticker desde celda B2 (fila 1, columna 1)
    ticker = str(df.iloc[1, 1]).strip()

    # Obtener nombre del archivo como Empresa (sin extensión)
    empresa = os.path.splitext(os.path.basename(ruta_csv))[0]

    # Extraer datos reales desde la fila 3 (índice 2)
    data = df.iloc[2:].reset_index(drop=True)

    # Asignar nombres de columna manualmente
    data.columns = ["Date", "Open", "High", "Low", "Close", "Adj Close", "Volume"]

    registros = []
    for _, row in data.iterrows():
        try:
            registro = {
                "Date": str(row["Date"]).strip(),  # SIN conversión de fecha
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

    # Guardar resultados en JSON
    nombre_salida = "bmv_stock_data.json" if carpeta == "datos_bmv" else "us_stock_data.json"
    ruta_salida = os.path.join(carpeta_destino, nombre_salida)
    with open(ruta_salida, "w", encoding="utf-8") as f:
        json.dump(registros_totales, f, indent=4, ensure_ascii=False)
    print(f"Guardado en: {ruta_salida}")

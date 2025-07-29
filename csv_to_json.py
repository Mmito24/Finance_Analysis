import os
import pandas as pd
import json

# Carpetas fuente y carpeta destino
carpetas_origen = ["datos_bmv", "datos_us"]
carpeta_destino = "datos_json"
os.makedirs(carpeta_destino, exist_ok=True)

def convertir_numero(valor):
    """Convierte strings a float o int si es posible, o retorna None si es 'nan' o vacío."""
    try:
        if pd.isna(valor) or str(valor).strip().lower() == 'nan':
            return None
        valor = float(valor)
        if valor.is_integer():
            return int(valor)
        return valor
    except:
        return None

def procesar_archivo(ruta_csv):
    # Leer sin encabezado
    df = pd.read_csv(ruta_csv, header=None)

    # Obtener Ticker desde celda B2 (fila 1, columna 1)
    ticker = str(df.iloc[1, 1]).strip()

    # Obtener nombre del archivo como Empresa (sin extensión)
    empresa = os.path.splitext(os.path.basename(ruta_csv))[0]

    # Extraer datos desde la fila 4 (índice 3)
    data = df.iloc[3:].reset_index(drop=True)

    # Asignar nombres de columna (ajustado "AdjClose" sin espacio)
    data.columns = ["Date", "Open", "High", "Low", "Close", "AdjClose", "Volume"]

    registros = []
    for _, row in data.iterrows():
        try:
            registro = {
                "Date": str(row["Date"]).strip(),  # Mantener como string
                "Open": convertir_numero(row["Open"]),
                "High": convertir_numero(row["High"]),
                "Low": convertir_numero(row["Low"]),
                "Close": convertir_numero(row["Close"]),
                "AdjClose": convertir_numero(row["AdjClose"]),
                "Volume": convertir_numero(row["Volume"]),
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


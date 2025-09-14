import pandas as pd
from typing import Optional
import json
from pathlib import Path
import os

class savePredictions:
    def __init__(self, datos: pd.DataFrame = None, ticker: str = None, nombre: str = None,bolsa:str = None):
        self._data_original = datos
        self.rutaSalida = Path(__file__).parent.parent.parent / "main" / "test" / "rawData" / "pronostico_de_acciones" / f"{bolsa}"
        self.ticker = ticker
        self.nombre = nombre

    def storagePredictions(self, data: Optional[pd.DataFrame] = None):

        datos_trabajo = data.copy() if data is not None else self._data_original.copy()

        if not datos_trabajo.empty:
            datos_trabajo['ticker'] = self.ticker
            # Usar solo pathlib.Path para consistencia
            subcarpeta = self.rutaSalida
            subcarpeta.mkdir(parents=True, exist_ok=True)
            ruta_archivo = subcarpeta / f"{self.nombre}_{self.ticker}.json"

            df_total = datos_trabajo

            # Crear lista documentos para guardar como NDJSON

            with open(ruta_archivo, 'w', encoding='utf-8') as f:
                for _, row in df_total.iterrows():
                    date_obj = pd.to_datetime(row['Date'], utc=True)

                    document = {
                        "Date": date_obj.strftime('%Y-%m-%d'),
                        "ticker": row['ticker'],
                        "frcst_sarimax_01_mean": float(row["frcst_sarimax_01_mean"]) if pd.notna(row["frcst_sarimax_01_mean"]) else None,
                        "frcst_sarimax_01_low": float(row["frcst_sarimax_01_low"]) if pd.notna(row["frcst_sarimax_01_low"]) else None,
                        "frcst_sarimax_01_upper": float(row["frcst_sarimax_01_upper"]) if pd.notna(row["frcst_sarimax_01_upper"]) else None
                    }
                    f.write(json.dumps(document, ensure_ascii=False) + "\n")

            print(
                f"[Almacenamiento Pronósticos] [{self.nombre}] ✅ Guardado como NDJSON compatible MongoDB en: {self.rutaSalida}")
            print(f"[Almacenamiento Pronósticos] Total documentos: {len(df_total)}")

        else:
            print(f"[Almacenamiento Pronósticos] [{self.nombre}] ⚠️ No se encontraron datos para {self.ticker}")

    def saveInJsonAssets(self,listaOrigen, listaDestino,bolsa):

        ruta_principal = Path(__file__).parent.parent.parent / "test"

        carpeta_origen = [ruta_principal / listaOrigen[0]]
        carpeta_destino = [ruta_principal / listaDestino[0]]

        # Carpetas fuente y carpeta destino

        os.makedirs(carpeta_destino[0], exist_ok=True)

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

        def procesar_archivo(ruta_json):
            # Leer sin encabezado
            df = pd.read_json(ruta_json, lines=True, convert_dates=['Date'])

            registros = []
            for _, row in df.iterrows():
                date_obj = pd.to_datetime(row['Date'], utc=True)

                try:
                    registro = {
                        "Date": date_obj.strftime('%Y-%m-%d'),
                        "ticker": row["ticker"],
                        "frcst_sarimax_01_mean": row["frcst_sarimax_01_mean"],
                        "frcst_sarimax_01_low": row["frcst_sarimax_01_low"],
                        "frcst_sarimax_01_upper" : row["frcst_sarimax_01_upper"]
                    }
                    registros.append(registro)
                except Exception as e:
                    print(f"Error procesando fila en {ruta_json}: {e}")
            return registros

        # Procesar carpetas

        def almacenarJson(carpeta_origen, carpeta_destino, nombre):
            registros_totales = []
            for archivo in os.listdir(carpeta_origen):
                if archivo.endswith(".json"):
                    ruta = os.path.join(carpeta_origen, archivo)
                    print(f"Procesando: {ruta}")
                    registros = procesar_archivo(ruta)
                    registros_totales.extend(registros)

                # Guardar resultados en JSON
                ruta_salida = os.path.join(carpeta_destino, nombre)
                with open(ruta_salida, "w", encoding="utf-8") as f:
                    json.dump(registros_totales, f, indent=4, ensure_ascii=False)
                print(f"Guardado en: {ruta_salida}")

        almacenarJson(carpeta_origen[0], carpeta_destino[0], f"{bolsa}_prediction.json")
import yfinance as yf
import os
from datetime import date, timedelta
from pathlib import Path
import json
import pandas as pd


class storage():
    def __init__(self,dictAssets = None,routeFile = None):
        self.dicAsset = dictAssets
        self.routeFile = routeFile

    def downloadAssetsPrices(self):

        hoy = date.today()
        hace_05_anios = hoy - timedelta(days=365 * 5)

        carpeta_salida = Path(__file__).parent.parent.parent / "test" / f"{self.routeFile}"

        os.makedirs(carpeta_salida, exist_ok=True)

        print(carpeta_salida)

        for ticker, nombre in self.dicAsset.items():
            print(f"Descargando datos de {nombre} ({ticker})...")

            # 🛠️ CORREGIDO: desactivamos el ajuste automático
            df = yf.download(ticker, start=hace_05_anios.isoformat(), end=hoy.isoformat(), auto_adjust=False)

            if not df.empty:
                columnas_deseadas = ['Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']
                columnas_presentes = [col for col in columnas_deseadas if col in df.columns]
                df_filtrado = df[columnas_presentes]

                ruta_archivo = os.path.join(carpeta_salida, f"{nombre}_{ticker}.csv")
                df_filtrado.to_csv(ruta_archivo)
                print(f"✅ Guardado en: {ruta_archivo}")
            else:
                print(f"⚠️ No se encontraron datos para {ticker}")

        print("\n✅ Descarga finalizada.")

    def downloadPertIndicator(self,nameFile):
        resultados = []

        carpeta_salida = Path(__file__).parent.parent.parent / "test" / f"{self.routeFile}"
        fichero = Path(__file__).parent.parent.parent / "test" / f"{self.routeFile}" / f"{nameFile}"
        for ticker_symbol, nombre in self.dicAsset.items():
            ticker = yf.Ticker(ticker_symbol)
            info = ticker.info

            precio = info.get('currentPrice')  # p=precio
            shares = info.get('sharesOutstanding')  # s=shares
            net_ttm = info.get('netIncomeToCommon')  # e=net_ttm

            fin = ticker.financials
            if 'Net Income' in fin.index:
                net_fiscal = fin.loc['Net Income'].iloc[0]
            else:
                net_fiscal = None

            def calc(e, s, p):
                if e and s and p:
                    eps = e / s  # calculo de ganancia por accion
                    per = p / eps if eps != 0 else None  # Relacion precio beneficio
                    return eps, per
                return None, None

            eps_ttm, per_ttm = calc(net_ttm, shares, precio)
            eps_fiscal, per_fiscal = calc(net_fiscal, shares, precio)

            resultados.append({
                "Empresa": nombre,
                "Ticker": ticker_symbol,
                "Price": precio,
                "Shares": shares,
                "Net Income TTM": net_ttm,
                "EPS TTM": eps_ttm,
                "PER TTM": per_ttm,
                "Net Income Fiscal": net_fiscal,
                "EPS Fiscal": eps_fiscal,
                "PER Fiscal": per_fiscal
            })

        # Guardar en JSON
        os.makedirs(carpeta_salida, exist_ok=True)

        with open(fichero, "w", encoding="utf-8") as f:
            json.dump(resultados, f, ensure_ascii=False, indent=4)

        print(f"Archivo {nameFile} generado correctamente.")

    def saveInJsonAssets(self,listaOrigen,listaDestino):

        ruta_principal = Path(__file__).parent.parent.parent / "test"

        carpeta_origen = [ruta_principal / listaOrigen[0], ruta_principal / listaOrigen[1]]
        carpeta_destino = [ruta_principal / listaDestino[0], ruta_principal / listaDestino[1]]

        # Carpetas fuente y carpeta destino

        os.makedirs(carpeta_destino[0], exist_ok=True)
        os.makedirs(carpeta_destino[1], exist_ok=True)

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

        def almacenarJson(carpeta_origen,carpeta_destino,nombre):
            registros_totales = []
            for archivo in os.listdir(carpeta_origen):
                if archivo.endswith(".csv"):
                    ruta = os.path.join(carpeta_origen, archivo)
                    print(f"Procesando: {ruta}")
                    registros = procesar_archivo(ruta)
                    registros_totales.extend(registros)

                # Guardar resultados en JSON
                ruta_salida = os.path.join(carpeta_destino, nombre)
                with open(ruta_salida, "w", encoding="utf-8") as f:
                    json.dump(registros_totales, f, indent=4, ensure_ascii=False)
                print(f"Guardado en: {ruta_salida}")

        almacenarJson(carpeta_origen[0],carpeta_destino[0],"us_stock_data.json")
        almacenarJson(carpeta_origen[1],carpeta_destino[1],"bmv_stock_data.json")


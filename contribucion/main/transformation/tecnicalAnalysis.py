import pandas as pd
from pandas import DataFrame
import numpy as np
import ta
from typing import Optional
from pathlib import Path
import json
import os


class tecnicalAnalysis:
    def __init__(self, dictAssets=None, routeFile=None):
        self.dicAsset = dictAssets
        self.routeFile = routeFile

    def calculateTradingIndicators(self,bolsa):

        for ticker, nombre in self.dicAsset.items():
            ruta = Path(__file__).parent.parent.parent / "main" / "test" / "rawData" / bolsa / f"{nombre}_{ticker}.csv"
            # ruta = f"test/rawData/{bolsa}/{nombre}_{ticker}.csv"
            df_raw = pd.read_csv(ruta, header=None)

            # --- Paso 1: usar la primera fila como encabezado ---
            new_header = df_raw.iloc[0]
            df = df_raw[1:]
            df.columns = new_header
            df = df.rename(columns={df.columns[0]: "Date"})
            df = df[df["Date"] != "Ticker"]
            df = df[df["Date"] != "Date"]
            df = df.reset_index(drop=True)

            df["Ticker"] = ticker

            for col in df.columns:
                if col not in ["Date", "Ticker"]:
                    df[col] = pd.to_numeric(df[col], errors="coerce")

            # Cálculo de las variables de rendimientos
            roiDf = self.indctr_01_roi(df)
            # Cálculo de las variables de volatilidad
            varDf = self.indctr_02_volatility(roiDf)
            # Cálculo de las variables de medias móviles exponenciales
            movingAverageExpDF = self.indctr_03_moving_average_exp(varDf)
            # Cálculo de las variables de medias móviles aritméticas
            movingAverageArDF = self.indctr_04_moving_average_ar(movingAverageExpDF)
            # Cálculo de las variables de tendencia
            trendIndicatosDF = self.indctr_05_trend_indicatos(movingAverageArDF)
            # Objeto para la escritura de las variables tecnicas calculadas
            print(f"\n[Almacenamiento Análisis Técnico] [{ticker}] [{nombre}] Completo ✅")

            self.storageTecnicalAnalysis(trendIndicatosDF, ticker, nombre,bolsa)



        self.saveInJsonAssets(
            [f"rawData/indicadores_de_trading/{bolsa}"],
            [f"dataBases/indicadores_de_trading/{bolsa}"],
            bolsa
        )




    def indctr_01_roi(self, data: Optional[pd.DataFrame] = None) -> pd.DataFrame:

        datos_trabajo = data.copy() if data is not None else self.data_original.copy()

        # Rendimiento aritmético (simple)
        datos_trabajo['rendimiento_aritmetico'] = datos_trabajo['Adj Close'].pct_change()

        # Rendimiento logarítmico
        datos_trabajo['rendimiento_logaritmico'] = np.log(datos_trabajo['Adj Close'] / datos_trabajo['Adj Close'].shift(1))

        # Rendimiento acumulado aritmético
        datos_trabajo['rendimiento_acumulado_arit'] = (1 + datos_trabajo['rendimiento_aritmetico']).cumprod() - 1

        # Rendimiento acumulado logarítmico
        datos_trabajo['rendimiento_acumulado_log'] = datos_trabajo['rendimiento_logaritmico'].cumsum()

        return datos_trabajo

    def indctr_02_volatility(self, data: Optional[pd.DataFrame] = None) -> pd.DataFrame:

        datos_trabajo = data.copy() if data is not None else self.data_original.copy()

        datos_trabajo['rendimiento_logaritmico'] = np.log(datos_trabajo['Adj Close'] / datos_trabajo['Adj Close'].shift(1))

        # Volatilidad móvil (desviación estándar de 005 días)
        datos_trabajo['volatilidad_005d'] = datos_trabajo['rendimiento_logaritmico'].rolling(window=5).std() * np.sqrt(252)  # Anualizada

        # Volatilidad móvil (desviación estándar de 010 días)
        datos_trabajo['volatilidad_010d'] = datos_trabajo['rendimiento_logaritmico'].rolling(window=10).std() * np.sqrt(252)  # Anualizada

        # Volatilidad móvil (desviación estándar de 015 días)
        datos_trabajo['volatilidad_015d'] = datos_trabajo['rendimiento_logaritmico'].rolling(window=15).std() * np.sqrt(252)  # Anualizada

        # Volatilidad móvil (desviación estándar de 020 días)
        datos_trabajo['volatilidad_020d'] = datos_trabajo['rendimiento_logaritmico'].rolling(window=20).std() * np.sqrt(252)  # Anualizada

        # Volatilidad móvil (desviación estándar de 030 días)
        datos_trabajo['volatilidad_030d'] = datos_trabajo['rendimiento_logaritmico'].rolling(window=30).std() * np.sqrt(252)  # Anualizada

        # Volatilidad móvil (desviación estándar de 060 días)
        datos_trabajo['volatilidad_060d'] = datos_trabajo['rendimiento_logaritmico'].rolling(window=60).std() * np.sqrt(252)  # Anualizada

        # Volatilidad móvil (desviación estándar de 090 días)
        datos_trabajo['volatilidad_090d'] = datos_trabajo['rendimiento_logaritmico'].rolling(window=90).std() * np.sqrt(252)  # Anualizada

        # Volatilidad móvil (desviación estándar de 252 días)
        datos_trabajo['volatilidad_252d'] = datos_trabajo['rendimiento_logaritmico'].rolling(window=252).std() * np.sqrt(252)  # Anualizada

        return datos_trabajo

    def indctr_03_moving_average_exp(self, data: Optional[pd.DataFrame] = None) -> pd.DataFrame:

        datos_trabajo = data.copy() if data is not None else self.data_original.copy()

        # Medias móviles de 5 días de corto plazo
        datos_trabajo['MA005'] = datos_trabajo['Adj Close'].rolling(window=5).mean()
        # Medias móviles de 10 días de corto plazo
        datos_trabajo['MA010'] = datos_trabajo['Adj Close'].rolling(window=10).mean()
        # Medias móviles de 12 días de corto plazo
        datos_trabajo['MA012'] = datos_trabajo['Adj Close'].rolling(window=12).mean()
        # Medias móviles de 20 días de mediano plazo
        datos_trabajo['MA020'] = datos_trabajo['Adj Close'].rolling(window=20).mean()
        # Medias móviles de 50 días de mediano plazo
        datos_trabajo['MA050'] = datos_trabajo['Adj Close'].rolling(window=50).mean()
        # Medias móviles de 60 días de mediano plazo
        datos_trabajo['MA060'] = datos_trabajo['Adj Close'].rolling(window=60).mean()
        # Medias móviles de 100 días de largo plazo
        datos_trabajo['MA100'] = datos_trabajo['Adj Close'].rolling(window=100).mean()
        # Medias móviles de 200 días de largo plazo
        datos_trabajo['MA200'] = datos_trabajo['Adj Close'].rolling(window=200).mean()

        return datos_trabajo

    def indctr_04_moving_average_ar(self, data: Optional[pd.DataFrame] = None) -> pd.DataFrame:

        datos_trabajo = data.copy() if data is not None else self.data_original.copy()

        # Medias móviles exponenciales de 5 días de corto plazo
        datos_trabajo['EMA005'] = datos_trabajo['Adj Close'].ewm(span=5, adjust=False).mean()
        # Medias móviles exponenciales de 10 días de corto plazo
        datos_trabajo['EMA010'] = datos_trabajo['Adj Close'].ewm(span=10, adjust=False).mean()
        # Medias móviles exponenciales de 12 días de corto plazo
        datos_trabajo['EMA012'] = datos_trabajo['Adj Close'].ewm(span=12, adjust=False).mean()
        # Medias móviles exponenciales de 20 días de mediano plazo
        datos_trabajo['EMA020'] = datos_trabajo['Adj Close'].ewm(span=20, adjust=False).mean()
        # Medias móviles exponenciales de 26 días de mediano plazo
        datos_trabajo['EMA026'] = datos_trabajo['Adj Close'].ewm(span=26, adjust=False).mean()
        # Medias móviles exponenciales de 50 días de mediano plazo
        datos_trabajo['EMA050'] = datos_trabajo['Adj Close'].ewm(span=50, adjust=False).mean()
        # Medias móviles exponenciales de 60 días de mediano plazo
        datos_trabajo['EMA060'] = datos_trabajo['Adj Close'].ewm(span=60, adjust=False).mean()
        # Medias móviles exponenciales de 100 días de largo plazo
        datos_trabajo['EMA100'] = datos_trabajo['Adj Close'].ewm(span=100, adjust=False).mean()
        # Medias móviles exponenciales de 200 días de largo plazo
        datos_trabajo['EMA200'] = datos_trabajo['Adj Close'].ewm(span=200, adjust=False).mean()

        return datos_trabajo

    def indctr_05_trend_indicatos(self, data: Optional[pd.DataFrame] = None) -> pd.DataFrame:

        datos_trabajo = data.copy() if data is not None else self.data_original.copy()

        # Medias móviles exponenciales de 26 días de mediano plazo
        datos_trabajo['EMA026'] = datos_trabajo['Adj Close'].ewm(span=26, adjust=False).mean()

        # Medias móviles exponenciales de 12 días de corto plazo
        datos_trabajo['EMA012'] = datos_trabajo['Adj Close'].ewm(span=12, adjust=False).mean()

        datos_trabajo['MACD'] = datos_trabajo['EMA012'] - datos_trabajo['EMA026']
        datos_trabajo['Signal_MACD'] = datos_trabajo['MACD'].ewm(span=9, adjust=False).mean()

        # Calcular ADX, +DI y -DI
        datos_trabajo['ADX'] = ta.trend.adx(datos_trabajo['High'], datos_trabajo['Low'], datos_trabajo['Adj Close'], window=14)
        datos_trabajo['ADX_DI_plus'] = ta.trend.adx_pos(datos_trabajo['High'], datos_trabajo['Low'], datos_trabajo['Adj Close'], window=14)
        datos_trabajo['ADX_DI_less'] = ta.trend.adx_neg(datos_trabajo['High'], datos_trabajo['Low'], datos_trabajo['Adj Close'], window=14)

        # Calcular el RSI (por ejemplo, de 14 días)
        datos_trabajo['RSI'] = ta.momentum.rsi(datos_trabajo['Adj Close'], window=14)

        # Calcular %K y %D
        datos_trabajo['stoch_k'] = ta.momentum.stoch(datos_trabajo['High'], datos_trabajo['Low'], datos_trabajo['Adj Close'], window=14, smooth_window=3)
        datos_trabajo['stoch_d'] = ta.momentum.stoch_signal(datos_trabajo['High'], datos_trabajo['Low'], datos_trabajo['Adj Close'], window=14, smooth_window=3)

        # Calcular la diferencia
        datos_trabajo['stoch_diff'] = datos_trabajo['stoch_k'] - datos_trabajo['stoch_d']

        # Calcular el CCI
        datos_trabajo['cci'] = ta.trend.cci(high=datos_trabajo['High'], low=datos_trabajo['Low'], close=datos_trabajo['Close'], window=20)

        # Calcular Bandas de Bollinger (20-periodos, 2 desviaciones)
        bb = ta.volatility.BollingerBands(close=datos_trabajo['Close'], window=20, window_dev=2)
        datos_trabajo['bb_upper'] = bb.bollinger_hband()
        datos_trabajo['bb_lower'] = bb.bollinger_lband()
        datos_trabajo['bb_middle'] = bb.bollinger_mavg()

        return datos_trabajo

    def storageTecnicalAnalysis(self, data: Optional[pd.DataFrame] = None,ticker = None, nombre = None,bolsa = None):

        datos_trabajo = data.copy()

        if not datos_trabajo.empty:
            ruta_archivo = Path(__file__).parent.parent.parent / "main" / "test" / "rawData" / "indicadores_de_trading" / f"{bolsa}" / f"{nombre}_{ticker}.json"
            # ruta_archivo = f"test/rawData/indicadores_de_trading/{bolsa}/{nombre}_{ticker}.json"
            df_total = datos_trabajo

            # Crear lista documentos para guardar como NDJSON

            data.head(5)

            with open(ruta_archivo, 'w', encoding='utf-8') as f:
                for _, row in data.iterrows():
                    date_obj = pd.to_datetime(row['Date'], utc=True)

                    document = {
                        "Date": date_obj.strftime('%Y-%m-%d'),
                        "ticker": ticker,
                        "Open": float(row['Open']) if pd.notna(row['Open']) else None,
                        "High": float(row['High']) if pd.notna(row['High']) else None,
                        "Low": float(row['Low']) if pd.notna(row['Low']) else None,
                        "Close": float(row['Close']) if pd.notna(row['Close']) else None,
                        "Adj Close": float(row['Adj Close']) if pd.notna(row['Adj Close']) else None,
                        "Volume": int(row['Volume']) if pd.notna(row['Volume']) else None,
                        "rendimiento_aritmetico": float(row["rendimiento_aritmetico"]) if pd.notna(row["rendimiento_aritmetico"]) else None,
                        "rendimiento_logaritmico": float(row["rendimiento_logaritmico"]) if pd.notna(row["rendimiento_logaritmico"]) else None,
                        "rendimiento_acumulado_arit": float(row["rendimiento_acumulado_arit"]) if pd.notna(row["rendimiento_acumulado_arit"]) else None,
                        "rendimiento_acumulado_log": float(row["rendimiento_acumulado_log"]) if pd.notna(row["rendimiento_acumulado_log"]) else None,
                        "volatilidad_005d": float(row["volatilidad_005d"]) if pd.notna(row["volatilidad_005d"]) else None,
                        "volatilidad_010d": float(row["volatilidad_010d"]) if pd.notna(row["volatilidad_010d"]) else None,
                        "volatilidad_015d": float(row["volatilidad_015d"]) if pd.notna(row["volatilidad_015d"]) else None,
                        "volatilidad_020d": float(row["volatilidad_020d"]) if pd.notna(row["volatilidad_020d"]) else None,
                        "volatilidad_030d": float(row["volatilidad_030d"]) if pd.notna(row["volatilidad_030d"]) else None,
                        "volatilidad_060d": float(row["volatilidad_060d"]) if pd.notna(row["volatilidad_060d"]) else None,
                        "volatilidad_090d": float(row["volatilidad_090d"]) if pd.notna(row["volatilidad_090d"]) else None,
                        "volatilidad_252d": float(row["volatilidad_252d"]) if pd.notna(row["volatilidad_252d"]) else None,
                        "MA005": float(row["MA005"]) if pd.notna(row["MA005"]) else None,
                        "MA010": float(row["MA010"]) if pd.notna(row["MA010"]) else None,
                        "MA012": float(row["MA012"]) if pd.notna(row["MA012"]) else None,
                        "MA020": float(row["MA020"]) if pd.notna(row["MA020"]) else None,
                        "MA050": float(row["MA050"]) if pd.notna(row["MA050"]) else None,
                        "MA060": float(row["MA060"]) if pd.notna(row["MA060"]) else None,
                        "MA100": float(row["MA100"]) if pd.notna(row["MA100"]) else None,
                        "MA200": float(row["MA200"]) if pd.notna(row["MA200"]) else None,
                        "EMA005": float(row["EMA005"]) if pd.notna(row["EMA005"]) else None,
                        "EMA010": float(row["EMA010"]) if pd.notna(row["EMA010"]) else None,
                        "EMA012": float(row["EMA012"]) if pd.notna(row["EMA012"]) else None,
                        "EMA020": float(row["EMA020"]) if pd.notna(row["EMA020"]) else None,
                        "EMA026": float(row["EMA026"]) if pd.notna(row["EMA026"]) else None,
                        "EMA050": float(row["EMA050"]) if pd.notna(row["EMA050"]) else None,
                        "EMA060": float(row["EMA060"]) if pd.notna(row["EMA060"]) else None,
                        "EMA100": float(row["EMA100"]) if pd.notna(row["EMA100"]) else None,
                        "EMA200": float(row["EMA200"]) if pd.notna(row["EMA200"]) else None,
                        "MACD": float(row["MACD"]) if pd.notna(row["MACD"]) else None,
                        "Signal_MACD": float(row["Signal_MACD"]) if pd.notna(row["Signal_MACD"]) else None,
                        "ADX": float(row["ADX"]) if pd.notna(row["ADX"]) else None,
                        "ADX_DI_plus": float(row["ADX_DI_plus"]) if pd.notna(row["ADX_DI_plus"]) else None,
                        "ADX_DI_less": float(row["ADX_DI_less"]) if pd.notna(row["ADX_DI_less"]) else None,
                        "RSI": float(row["RSI"]) if pd.notna(row["RSI"]) else None,
                        "stoch_k": float(row["stoch_k"]) if pd.notna(row["stoch_k"]) else None,
                        "stoch_d": float(row["stoch_d"]) if pd.notna(row["stoch_d"]) else None,
                        "stoch_diff": float(row["stoch_diff"]) if pd.notna(row["stoch_diff"]) else None,
                        "cci": float(row["cci"]) if pd.notna(row["cci"]) else None,
                        "bb_upper": float(row["bb_upper"]) if pd.notna(row["bb_upper"]) else None,
                        "bb_lower": float(row["bb_lower"]) if pd.notna(row["bb_lower"]) else None,
                        "bb_middle": float(row["bb_middle"]) if pd.notna(row["bb_middle"]) else None
                    }
                    f.write(json.dumps(document, ensure_ascii=False) + "\n")

            print(f"[Almacenamiento Análisis Técnico] [{nombre}] ✅ ")
            print(f"[Almacenamiento Análisis Técnico] Total documentos: {len(df_total)}")

        else:
            print(f"[Almacenamiento Análisis Técnico] [{nombre}] ⚠️ No se encontraron datos para {ticker}")

    def saveInJsonAssets(self, listaOrigen, listaDestino,bolsa):

        ruta_principal = Path(__file__).parent.parent.parent / "main" / "test"

        # ruta_principal = "test"

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
                        "Open": float(row['Open']) if pd.notna(row['Open']) else None,
                        "High": float(row['High']) if pd.notna(row['High']) else None,
                        "Low": float(row['Low']) if pd.notna(row['Low']) else None,
                        "Close": float(row['Close']) if pd.notna(row['Close']) else None,
                        "Adj Close": float(row['Adj Close']) if pd.notna(row['Adj Close']) else None,
                        "Volume": int(row['Volume']) if pd.notna(row['Volume']) else None,
                        "rendimiento_aritmetico": float(row["rendimiento_aritmetico"]) if pd.notna(row["rendimiento_aritmetico"]) else None,
                        "rendimiento_logaritmico": float(row["rendimiento_logaritmico"]) if pd.notna(row["rendimiento_logaritmico"]) else None,
                        "rendimiento_acumulado_arit": float(row["rendimiento_acumulado_arit"]) if pd.notna(row["rendimiento_acumulado_arit"]) else None,
                        "rendimiento_acumulado_log": float(row["rendimiento_acumulado_log"]) if pd.notna(row["rendimiento_acumulado_log"]) else None,
                        "volatilidad_005d": float(row["volatilidad_005d"]) if pd.notna(row["volatilidad_005d"]) else None,
                        "volatilidad_010d": float(row["volatilidad_010d"]) if pd.notna(row["volatilidad_010d"]) else None,
                        "volatilidad_015d": float(row["volatilidad_015d"]) if pd.notna(row["volatilidad_015d"]) else None,
                        "volatilidad_020d": float(row["volatilidad_020d"]) if pd.notna(row["volatilidad_020d"]) else None,
                        "volatilidad_030d": float(row["volatilidad_030d"]) if pd.notna(row["volatilidad_030d"]) else None,
                        "volatilidad_060d": float(row["volatilidad_060d"]) if pd.notna(row["volatilidad_060d"]) else None,
                        "volatilidad_090d": float(row["volatilidad_090d"]) if pd.notna(row["volatilidad_090d"]) else None,
                        "volatilidad_252d": float(row["volatilidad_252d"]) if pd.notna(row["volatilidad_252d"]) else None,
                        "MA005": float(row["MA005"]) if pd.notna(row["MA005"]) else None,
                        "MA010": float(row["MA010"]) if pd.notna(row["MA010"]) else None,
                        "MA012": float(row["MA012"]) if pd.notna(row["MA012"]) else None,
                        "MA020": float(row["MA020"]) if pd.notna(row["MA020"]) else None,
                        "MA050": float(row["MA050"]) if pd.notna(row["MA050"]) else None,
                        "MA060": float(row["MA060"]) if pd.notna(row["MA060"]) else None,
                        "MA100": float(row["MA100"]) if pd.notna(row["MA100"]) else None,
                        "MA200": float(row["MA200"]) if pd.notna(row["MA200"]) else None,
                        "EMA005": float(row["EMA005"]) if pd.notna(row["EMA005"]) else None,
                        "EMA010": float(row["EMA010"]) if pd.notna(row["EMA010"]) else None,
                        "EMA012": float(row["EMA012"]) if pd.notna(row["EMA012"]) else None,
                        "EMA020": float(row["EMA020"]) if pd.notna(row["EMA020"]) else None,
                        "EMA026": float(row["EMA026"]) if pd.notna(row["EMA026"]) else None,
                        "EMA050": float(row["EMA050"]) if pd.notna(row["EMA050"]) else None,
                        "EMA060": float(row["EMA060"]) if pd.notna(row["EMA060"]) else None,
                        "EMA100": float(row["EMA100"]) if pd.notna(row["EMA100"]) else None,
                        "EMA200": float(row["EMA200"]) if pd.notna(row["EMA200"]) else None,
                        "MACD": float(row["MACD"]) if pd.notna(row["MACD"]) else None,
                        "Signal_MACD": float(row["Signal_MACD"]) if pd.notna(row["Signal_MACD"]) else None,
                        "ADX": float(row["ADX"]) if pd.notna(row["ADX"]) else None,
                        "ADX_DI_plus": float(row["ADX_DI_plus"]) if pd.notna(row["ADX_DI_plus"]) else None,
                        "ADX_DI_less": float(row["ADX_DI_less"]) if pd.notna(row["ADX_DI_less"]) else None,
                        "RSI": float(row["RSI"]) if pd.notna(row["RSI"]) else None,
                        "stoch_k": float(row["stoch_k"]) if pd.notna(row["stoch_k"]) else None,
                        "stoch_d": float(row["stoch_d"]) if pd.notna(row["stoch_d"]) else None,
                        "stoch_diff": float(row["stoch_diff"]) if pd.notna(row["stoch_diff"]) else None,
                        "cci": float(row["cci"]) if pd.notna(row["cci"]) else None,
                        "bb_upper": float(row["bb_upper"]) if pd.notna(row["bb_upper"]) else None,
                        "bb_lower": float(row["bb_lower"]) if pd.notna(row["bb_lower"]) else None,
                        "bb_middle": float(row["bb_middle"]) if pd.notna(row["bb_middle"]) else None
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

        almacenarJson(carpeta_origen[0], carpeta_destino[0], f"{bolsa}_ind_trading.json")

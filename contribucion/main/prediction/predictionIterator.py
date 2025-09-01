from pathlib import Path
import pandas as pd

from SARIMAXmodel import SARIMAXmodel
from savePredictions import savePredictions


def predictionIterator(empresas_bmv,bolsa):
    for ticker, nombre in empresas_bmv.items():
        ruta = Path(__file__).parent.parent.parent / "test" / "rawData" / bolsa / f"{nombre}_{ticker}.csv"
        df_raw = pd.read_csv(ruta, header=None)

        # Obtener Ticker desde celda B2 (fila 1, columna 1)
        ticker = str(df_raw.iloc[1, 1]).strip()

        # Obtener nombre del archivo como Empresa (sin extensión)
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

        prediccion = SARIMAXmodel(df, nombre)

        pronostico = prediccion.pronosticar_sarimax(60)
        evaluacion = prediccion.evaluar_modelo()
        almacenamientoPronosticos = savePredictions(pronostico, ticker, nombre,bolsa)
        almacenamientoPronosticos.storagePredictions()

    savePredictions().saveInJsonAssets(
        listaOrigen = [f"rawData/pronostico_de_acciones/{bolsa}"],
        listaDestino = [f"dataBases/pronostico_de_acciones/{bolsa}"],
        bolsa = bolsa
    )
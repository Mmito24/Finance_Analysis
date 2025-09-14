from pathlib import Path
import os

class deleteFiles:
    def __init__(self, empresas_bmv, bolsa):
        self.empresas_bmv = empresas_bmv
        self.bolsa = bolsa

    def iteratorDeleteFile(self):
        try:
            for ticker, nombre in self.empresas_bmv.items():
                ruta = Path(__file__).parent.parent.parent / "main" / "test" / "rawData" / self.bolsa / f"{nombre}_{ticker}.csv"
                # ruta = f"test/rawData/{self.bolsa}/{nombre}_{ticker}.csv"
                if ruta.is_file():
                    os.remove(ruta)
                    print(f"Archivo eliminado: {ruta.name}")

                ruta = Path(__file__).parent.parent.parent / "main" / "test" / "rawData" / "indicadores_de_trading" / self.bolsa / f"{nombre}_{ticker}.json"
                # ruta = f"test/rawData/indicadores_de_trading/{self.bolsa}/{nombre}_{ticker}.json"
                if ruta.is_file():
                    os.remove(ruta)
                    print(f"Archivo eliminado: {ruta.name}")

                ruta = Path(__file__).parent.parent.parent / "main" / "test" / "rawData" / "pronostico_de_acciones" / self.bolsa / f"{nombre}_{ticker}.json"
                # ruta = f"test/rawData/pronostico_de_acciones/{self.bolsa}/{nombre}_{ticker}.json"
                if ruta.is_file():
                    os.remove(ruta)
                    print(f"Archivo eliminado: {ruta.name}")

        except FileNotFoundError:
            print(f"Error: La ruta '{ruta}' no se encontró.")

        except Exception as e:
            print(f"Ocurrió un error inesperado: {e}")
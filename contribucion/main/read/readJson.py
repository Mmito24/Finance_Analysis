import json
from typing import Dict
from pathlib import Path

class readJson():
    def __init__(self,nombre):
        self.name = nombre

    def readTickers(self) -> Dict[str, str]:
        """
        Lee el ticker y el nombre de la empresa, regresa un diccionario para iterar
        """
        ruta = Path(__file__).parent.parent.parent / "main" / "test" / "empresas" / f"{self.name}"
        # ruta = f"test/empresas/{self.name}"

        diccionario_empresas = {}
        print(f"\nLeyendo desde '{ruta}'...")

        try:
            with open(ruta, 'r', encoding='utf-8') as f:
                for linea in f:
                    if linea.strip():
                        empresa_obj = json.loads(linea)
                        ticker = empresa_obj['ticker']
                        nombre = empresa_obj['nombre']
                        diccionario_empresas[ticker] = nombre

        except FileNotFoundError:
            print(f"⚠️ Error: El archivo '{ruta}' no existe.")
            return None
        except json.JSONDecodeError as e:
            print(f"⚠️ Error: El archivo contiene JSON inválido en la línea: {linea.strip()} ({e})")
            return None

        return diccionario_empresas
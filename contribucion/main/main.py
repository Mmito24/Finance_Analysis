from read.readJson import readJson
from storage.storage import storage

def main():
    # Lectura de los tickers y nombres
    dictBEU = readJson("bolsa_estados_unidos.json").readTickers()
    dictBMV = readJson("bolsa_mexicana_de_valores.json").readTickers()

    # Almacenamiento de las acciones en carpeta rawData
    storage(dictBEU,"rawData\\bolsa_estados_unidos").downloadAssetsPrices()
    storage(dictBMV,"rawData\\bolsa_mexicana_de_valores").downloadAssetsPrices()

    # Cálculo del PER
    storage(dictBEU, "dataBases\\indicadores_de_acciones").downloadPertIndicator("FinanzPER_BEU.json")
    storage(dictBMV, "dataBases\\indicadores_de_acciones").downloadPertIndicator("FinanzPER_BMV.json")

    # Unión de precios de activos
    storage().saveInJsonAssets(
        ["rawData\\bolsa_estados_unidos","rawData\\bolsa_mexicana_de_valores"],
        ["dataBases\\bolsa_estados_unidos","dataBases\\bolsa_mexicana_de_valores"]
    )

main()

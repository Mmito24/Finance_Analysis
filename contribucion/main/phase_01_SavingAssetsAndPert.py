from storage.storage import storage

def main():

    # Lectura de los tickers y nombres
    # dictBEU = readJson("bolsa_estados_unidos.json").readTickers()
    # dictBMV = readJson("bolsa_mexicana_de_valores.json").readTickers()


    dictBEU = {
        "NVDA": "NVIDIA Corporation",
        "MSFT": "Microsoft Corporation",
        "AAPL": "Apple Inc.",
        "AMZN": "Amazon.com Inc.",
        "GOOGL": "Alphabet Inc.",
        "META": "Meta Platforms Inc.",
        "AVGO": "Broadcom Inc.",
        "TSLA": "Tesla Inc.",
        "BRK-B": "Berkshire Hathaway Inc.",
        "TSM": "Taiwan Semiconductor Manufacturing Company Limited",
        "JPM": "JPMorgan Chase & Co.",
        "WMT": "Walmart Inc.",
        "LLY": "Eli Lilly and Company",
        "ORCL": "Oracle Corporation",
        "V": "Visa Inc.",
        "NFLX": "Netflix Inc.",
        "MA": "Mastercard Incorporated",
        "XOM": "Exxon Mobil Corporation",
        "COST": "Costco Wholesale Corporation",
        "JNJ": "Johnson & Johnson",
        "PG": "The Procter & Gamble Company",
        "PLTR": "Palantir Technologies Inc.",
        "SAP": "SAP SE",
        "HD": "The Home Depot Inc.",
        "BAC": "Bank of America Corporation",
        "IBKR": "Interactive Brokers Group Inc.",
        "ABBV": "AbbVie Inc.",
        "KO": "The Coca-Cola Company",
        "NVO": "Novo Nordisk A_S",
        "ASML": "ASML Holding N.V.",
        "GE": "General Electric Company",
        "PM": "Philip Morris International Inc.",
        "BABA": "Alibaba Group Holding Limited",
        "CSCO": "Cisco Systems Inc.",
        "IBM": "International Business Machines Corporation",
        "CVX": "Chevron Corporation",
        "WFC": "Wells Fargo & Company",
        "TMUS": "T_Mobile US Inc.",
        "UNH": "UnitedHealth Group Incorporated",
        "AMD": "Advanced Micro Devices Inc.",
        "CRM": "Salesforce Inc.",
        "MS": "Morgan Stanley",
        "NVS": "Novartis AG",
        "TM": "Toyota Motor Corporation",
        "LIN": "Linde plc",
        "DIS": "The Walt Disney Company",
        "HSBC": "HSBC Holdings plc",
        "AXP": "American Express Company",
        "ABT": "Abbott Laboratories",
        "AZN": "AstraZeneca PLC",
        "MCD": "McDonald's Corporation",
        "INTU": "Intuit Inc.",
        "GS": "The Goldman Sachs Group Inc.",
        "SHEL": "Shell plc",
        "BX": "Blackstone Inc.",
        "RTX": "RTX Corporation",
        "MRK": "Merck & Co. Inc.",
        "NOW": "ServiceNow Inc.",
        "TXN": "Texas Instruments Incorporated",
        "PEP": "PepsiCo Inc.",
        "CAT": "Caterpillar Inc.",
        "T": "AT&T Inc.",
        "UBER": "Uber Technologies Inc."
    }

    dictBMV = {
        "AMXB.MX": "AmericaMovil",
        "WALMEX.MX": "WalmartMexico",
        "GFNORTEO.MX": "Banorte",
        "BIMBOA.MX": "Bimbo",
        "TLEVISACPO.MX": "Televisa",
        "ALSEA.MX": "Alsea",
        "CEMEXCPO.MX": "Cemex",
        "FEMSAUBD.MX": "Femsa",
        "PE&OLES.MX": "Peñoles",
        "KIMBERA.MX": "KimberlyClark",
        "GMEXICOB.MX": "GrupoMexico",
        "FEMSAUB.MX": "CocaColaFEMSAUB",
        "KOFUBL.MX": "CocaColaFEMSAL",
        "AC.MX": "ArcaContinental",
        "GCARSOA1.MX": "GrupoCarsoA1",
        "GFINBURO.MX": "GpoFinbursa",
        "GAPB.MX": "GrupoAeroportuarioPacificoB",
        "ASURB.MX": "AsurB",
        "GMXT.MX": "GMexicoTransportes",
        "CHDRAUIB.MX": "ChedrauiB",
        "LIVEPOL1.MX": "ElPuertodeLiverpool",
        "LIVEPOLC-1.MX": "ElPuertodeLiverpoolC",
        "FIBRAPL14.MX": "PrologisPropertyMexico",
        "KIMBERB.MX": "Kimberly-ClarkMexicoB",
        "VISTAA.MX": "VistaOilGas",
        "FUNO11.MX": "FibraUnoAdministracion",
        "OMAB.MX": "GrupoAeroportuarioDelCentroNorte",
        "PINFRAL.MX": "PromotorayOperadoraInfraestrL",
        "PINFRA.MX": "PromotorayOperadoraInfraestr",
        "ELEKTRA.MX": "GrupoElektra",
        "Q.MX": "QualitasControladora",
        "ALFAA.MX": "Alfa_A",
        "ICHB.MX": "IndustriasCH",
        "EDUCA18.MX": "Grupo Nagoin",
        "CMOCTEZ.MX": "Moctezuma",
        "GENTERA.MX": "Gentera",
        "GCC.MX": "GCC",
        "BBAJIOO.MX": "BancodelBajio",
        "FRAGUAB.MX": "FRAGUAB",
        "RA.MX": "Regional",
        "SORIANAB.MX": "OrganizacionSoriana",
        "MEGACPO.MX": "MegacableCpo",
        "VESTA.MX": "CorporacionInmobiliariaVesta",
        "LACOMERUBC.MX": "LaComercial",
        "ALSEA.MX": "Alsea",
        "LAMOSA.MX": "Lamosa",
        "GPROFUT.MX": "GrupoProfuturo",
        "GIGANTE.MX": "Gigante",
        "GNP.MX": "Grupo Nacional Provincial",
        "HERDEZ.MX": "Herdez",
        "LABB.MX": "GenommaLabB",
        "BOLSAA.MX": "BolsaMexicanaValores"
    }

    # Almacenamiento de las acciones en carpeta rawData
    storage(dictBEU, "rawData/bolsa_estados_unidos").downloadAssetsPrices()
    storage(dictBMV, "rawData/bolsa_mexicana_de_valores").downloadAssetsPrices()

    # Cálculo del PER
    storage(dictBEU, "dataBases/indicadores_de_acciones").downloadPertIndicator("FinanzPER_BEU.json")
    storage(dictBMV, "dataBases/indicadores_de_acciones").downloadPertIndicator("FinanzPER_BMV.json")

    # Unión de precios de activos
    storage().saveInJsonAssets(
        ["rawData/bolsa_estados_unidos", "rawData/bolsa_mexicana_de_valores"],
        ["dataBases/bolsa_estados_unidos", "dataBases/bolsa_mexicana_de_valores"]
    )

main()
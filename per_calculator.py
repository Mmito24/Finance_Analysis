import yfinance as yf
import pandas as pd
import json
import os

# Diccionario de empresas
empresas = {
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

resultados = []

for ticker_symbol, nombre in empresas.items():
    ticker = yf.Ticker(ticker_symbol)
    info = ticker.fast_info
    
    precio = info.get('currentPrice') #p=precio
    shares = info.get('sharesOutstanding') #s=shares
    net_ttm = info.get('netIncomeToCommon') #e=net_ttm
    
    fin = ticker.financials
    if 'Net Income' in fin.index:
        net_fiscal = fin.loc['Net Income'].iloc[0]
    else:
        net_fiscal = None
    
    def calc(e, s, p):
        if e and s and p:
            eps = e / s #calculo de ganancia por accion
            per = p / eps if eps != 0 else None # Relacion precio beneficio
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
os.makedirs("datos_json", exist_ok=True)
with open("datos_json/FinanzPER.json", "w", encoding="utf-8") as f:
    json.dump(resultados, f, ensure_ascii=False, indent=4)

print("Archivo FinanzPER.json generado correctamente.")

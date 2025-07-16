import yfinance as yf
import os
from datetime import date, timedelta

empresas_bmv = {
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
    "LABB.MX": "GenommaLabB"
}

hoy = date.today()
hace_10_anios = hoy - timedelta(days=365 * 10)

carpeta_salida = "datos_bmv"
os.makedirs(carpeta_salida, exist_ok=True)

for ticker, nombre in empresas_bmv.items():
    print(f"Descargando datos de {nombre} ({ticker})...")
    
    # 🛠️ CORREGIDO: desactivamos el ajuste automático
    df = yf.download(ticker, start=hace_10_anios.isoformat(), end=hoy.isoformat(), auto_adjust=False)
    
    if not df.empty:
        columnas_deseadas = ['Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']
        columnas_presentes = [col for col in columnas_deseadas if col in df.columns]
        df_filtrado = df[columnas_presentes]
        
        ruta_archivo = os.path.join(carpeta_salida, f"{nombre}_{ticker.replace('.MX', '')}.csv")
        df_filtrado.to_csv(ruta_archivo)
        print(f"✅ Guardado en: {ruta_archivo}")
    else:
        print(f"⚠️ No se encontraron datos para {ticker}")

print("\n✅ Descarga finalizada.")
import yfinance as yf
import os
from datetime import date, timedelta

empresas_US = {
    "NVDA": "NVIDIA Corporation",
    "MSFT": "Microsoft Corporation",
    "AAPL": "Apple Inc.",
    "AMZN": "Amazon.com Inc.",
    "GOOGL": "Alphabet Inc.",
    "META": "Meta Platforms Inc.",
    "AVGO": "Broadcom Inc.",
    "TSLA": "Tesla Inc.",
    "BRK.B": "Berkshire Hathaway Inc.",
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
    "UBER": "Uber Technologies Inc.",
    "RY": "Royal Bank of Canada",
    "ISRG": "Intuitive Surgical Inc.",
    "BKNG": "Booking Holdings Inc.",
    "ACN": "Accenture plc",
    "HDB": "HDFC Bank Limited",
    "SCHW": "The Charles Schwab Corporation",
    "BA": "The Boeing Company",
    "VZ": "Verizon Communications Inc.",
    "C": "Citigroup Inc.",
    "BLK": "BlackRock Inc.",
    "QCOM": "QUALCOMM Incorporated",
    "ARM": "Arm Holdings plc",
    "SHOP": "Shopify Inc.",
    "SPGI": "S&P Global Inc.",
    "AMGN": "Amgen Inc.",
    "GEV": "GE Vernova Inc.",
    "TMO": "Thermo Fisher Scientific Inc.",
    "NEE": "NextEra Energy Inc.",
    "ADBE": "Adobe Inc.",
    "PDD": "PDD Holdings Inc.",
    "BSX": "Boston Scientific Corporation",
    "AMAT": "Applied Materials Inc.",
    "HON": "Honeywell International Inc.",
    "MUFG": "Mitsubishi UFJ Financial Group Inc.",
    "SYK": "Stryker Corporation",
    "UL": "Unilever PLC",
    "ETN": "Eaton Corporation plc",
    "PGR": "The Progressive Corporation",
    "SONY": "Sony Group Corporation",
    "SPOT": "Spotify Technology S.A.",
    "ANET": "Arista Networks Inc",
    "PFE": "Pfizer Inc.",
    "TTE": "TotalEnergies SE",
    "TJX": "The TJX Companies Inc.",
    "DHR": "Danaher Corporation",
    "DE": "Deere & Company",
    "GILD": "Gilead Sciences Inc."
}

hoy = date.today()
hace_10_anios = hoy - timedelta(days=365 * 10)

carpeta_salida = "datos_US"
os.makedirs(carpeta_salida, exist_ok=True)

for ticker, nombre in empresas_US.items():
    print(f"Descargando datos de {nombre} ({ticker})...")
    
    # 🛠️ CORREGIDO: desactivamos el ajuste automático
    df = yf.download(ticker, start=hace_10_anios.isoformat(), end=hoy.isoformat(), auto_adjust=False)
    
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
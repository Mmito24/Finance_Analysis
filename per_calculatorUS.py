import yfinance as yf
import pandas as pd
import json
import os

# Diccionario de empresas
empresas_EEUU = {
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

resultados = []

for ticker_symbol, nombre in empresas_EEUU.items():
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
with open("datos_json/FinanzPER_US.json", "w", encoding="utf-8") as f:
    json.dump(resultados, f, ensure_ascii=False, indent=4)

print("Archivo FinanzPER_US.json generado correctamente.")

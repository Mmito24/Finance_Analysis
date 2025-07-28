import os
import csv
import json

# Carpetas de origen
carpeta_bmv = 'datos_bmv'
carpeta_us = 'datos_us'

# Archivos de salida
archivo_salida_bmv = 'datos_json/bmv_stock_data.json'
archivo_salida_us = 'datos_json/us_stock_data.json'

# Asegurar que la carpeta de salida exista
os.makedirs('datos_json', exist_ok=True)

def procesar_csvs_en_carpeta(carpeta):
    datos = []
    for archivo in os.listdir(carpeta):
        if archivo.endswith('.csv'):
            ruta = os.path.join(carpeta, archivo)
            try:
                with open(ruta, newline='', encoding='utf-8') as f:
                    lector = csv.DictReader(f)
                    for fila in lector:
                        fila['archivo_origen'] = archivo  # metadato adicional
                        datos.append(fila)
            except Exception as e:
                print(f"Error al procesar {archivo}: {e}")
    return datos

# Procesar ambas carpetas
datos_bmv = procesar_csvs_en_carpeta(carpeta_bmv)
datos_us = procesar_csvs_en_carpeta(carpeta_us)

# Guardar los resultados
with open(archivo_salida_bmv, 'w', encoding='utf-8') as f:
    json.dump(datos_bmv, f, indent=2, ensure_ascii=False)

with open(archivo_salida_us, 'w', encoding='utf-8') as f:
    json.dump(datos_us, f, indent=2, ensure_ascii=False)

print("Archivos JSON generados exitosamente.")

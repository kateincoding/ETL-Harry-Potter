#!/usr/bin/env python3
# Script para ejecutar solo la fase de Load
# Load es independiente: Lee transformed_data.json y lo carga en MongoDB

import json
import os
import time
from load import LoadHpToMongo

if __name__ == "__main__":
    print("Trigger del transformed_data. file: Esperando a que extract genere el archivo...")
    path = "/app/data/2.transformed_data.json"
    while not os.path.exists(path):
        time.sleep(2)
    print("Se encontro 2.transformed_data encontrado, iniciamos el load")

    # Lectura de la data 
    data_dir = os.getenv('DATA_DIR', '/app/data')
    input_file = os.path.join(data_dir, '2.transformed_data.json')
    
    if not os.path.exists(input_file):
        print(f"Error: No se encontró {input_file}")
        exit(1)
    
    with open(input_file, 'r') as f:
        transformed_data = json.load(f)
    
    # Cargar en MongoDB
    mongo_connection = os.getenv('MONGO_CONNECTION', 'mongodb://localhost:27017/')
    loader = LoadHpToMongo(url_conexion=mongo_connection)
    results = loader.load_all(transformed_data, replace=True)
    
    print(f"\n✓ Datos cargados en MongoDB")
    print(f"  Characters: {results['characters']} documentos")

#!/usr/bin/env python3
# Script para ejecutar solo la fase de Transform

import json
import os
from transform import HPTransformer

if __name__ == "__main__":
    # Leer datos extraídos
    data_dir = os.getenv('DATA_DIR', '/app/data')
    input_file = os.path.join(data_dir, 'raw_data.json')
    
    if not os.path.exists(input_file):
        print(f"Error: No se encontró {input_file}")
        exit(1)
    
    with open(input_file, 'r') as f:
        raw_data = json.load(f)
    
    # Transformar datos
    transformer = HPTransformer(load_to_mongo=False)
    transformed_data = transformer.transform_all(raw_data, load_to_mongo=False)
    
    # Guardar datos transformados
    output_file = os.path.join(data_dir, 'transformed_data.json')
    with open(output_file, 'w') as f:
        json.dump(transformed_data, f, indent=2)
    
    print(f"\n✓ Datos transformados guardados en {output_file}")
    print(f"  Characters: {len(transformed_data['characters'])}")

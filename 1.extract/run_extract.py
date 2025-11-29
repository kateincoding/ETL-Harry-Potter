#!/usr/bin/env python3
# Script para ejecutar solo la fase de Extract

import json
import os
from extract import SWAPIExtractorBase

if __name__ == "__main__":
    extractor = SWAPIExtractorBase()
    data = extractor.extract_all()
    
    # Guardar datos en JSON
    output_dir = os.getenv('OUTPUT_DIR', '/app/data')
    os.makedirs(output_dir, exist_ok=True)
    
    output_file = os.path.join(output_dir, 'raw_data.json')
    with open(output_file, 'w') as f:
        json.dump(data, f, indent=2)
    
    print(f"\n✓ Datos extraídos guardados en {output_file}")
    print(f"  People: {len(data['people'])}")
    print(f"  Planets: {len(data['planets'])}")
    print(f"  Starships: {len(data['starships'])}")


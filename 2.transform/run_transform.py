#!/usr/bin/env python3
# Script para ejecutar solo la fase de Transform
# Transform es independiente: Lee 1.raw_data.json y genera 2.transformed_data.json y analysis_report.json

import json
import time
import os
from transform import HPTransformer, DescriptiveAnalysis

if __name__ == "__main__":
    # inicializamos el transform una vez el extract haya generado el archivo
    print("Trigger del raw_data file: Esperando a que extract genere el archivo...")
    path = "/app/data/1.raw_data.json"
    while not os.path.exists(path):
        time.sleep(2)
    print("Se encontro raw_data encontrado, iniciamos el transform")

    data_directory = os.getenv('DATA_DIR', '/app/data')
    input_file = os.path.join(data_directory, '1.raw_data.json')
    
    if not os.path.exists(input_file):
        print(f"Error: No se encontró {input_file}")
        exit(1)
    
    with open(input_file, 'r') as f:
        raw_data = json.load(f)
    
    # Aqui llamamos a la parte de transformación de datos
    transformer = HPTransformer()
    transformed_data = transformer.transform_all(raw_data)
    
    output_file = os.path.join(data_directory, '2.transformed_data.json')
    with open(output_file, 'w') as f:
        json.dump(transformed_data, f, indent=2)
    
    print(f"\n✓ Datos transformados guardados en {output_file}")
    print(f"  Characters: {len(transformed_data['characters'])}")
    
    # Aqui generamos el análisis descriptivo
    if transformed_data['characters']:
        analysis = DescriptiveAnalysis(transformed_data['characters'])
        report_file = os.path.join(data_directory, 'analysis_report.json')
        if analysis.save_report(report_file, dependent_variable='house', top_n=4):
            print(f"✓ Reporte de análisis guardado en {report_file}")
        else:
            print(f"✗ Error al guardar reporte de análisis")

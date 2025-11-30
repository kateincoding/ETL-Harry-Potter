# Transformación de datos y carga en MongoDB

import json
import logging
import os
from typing import Dict, List, Optional

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class HPTransformer:
    def __init__(self):
        """Inicializa el transformador. Transform es independiente de MongoDB."""
        pass
    
    def _parse_numeric(self, value) -> Optional[float]:
        """Convierte un valor a float si es posible"""
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            if value.lower() in ['unknown', 'n/a', 'none', '']:
                return None
            try:
                cleaned = value.replace(',', '')
                return float(cleaned)
            except (ValueError, AttributeError):
                return None
        return None
    
    def transform_characters(self, characters_data: List[Dict]) -> List[Dict]:
        """Transforma los datos de personajes de Harry Potter"""
        logger.info(f"Transformando {len(characters_data)} characters...")
        
        transformed = []
        for character in characters_data:
            try:
                # Extraer información de la varita
                wand = character.get('wand', {})
                
                transformed_character = {
                    'id': character.get('id'),
                    'name': character.get('name'),
                    'alternate_names': character.get('alternate_names', []),
                    'house': character.get('house'),
                    'year_of_birth': self._parse_numeric(character.get('yearOfBirth')),
                    'ancestry': character.get('ancestry'),  # pureza de sangre
                    'gender': character.get('gender'),  # male/female
                    'species': character.get('species'),
                    'wizard': character.get('wizard'),
                    # Información de la varita
                    'wand': {
                        'wood': wand.get('wood'),  # material de la madera de la varita
                        'core': wand.get('core'),  # núcleo de la varita
                        'length': self._parse_numeric(wand.get('length')) 
                    },
                    # Información adicional del personaje
                    'patronus': character.get('patronus'),
                    'hogwarts_student': character.get('hogwartsStudent'),
                    'hogwarts_staff': character.get('hogwartsStaff'),
                    'actor': character.get('actor'),
                    'alternate_actors': character.get('alternate_actors', []),
                    'alive': character.get('alive'),
                    'image': character.get('image'),
                    'eye_colour': character.get('eyeColour'),
                    'hair_colour': character.get('hairColour'),
                    'date_of_birth': character.get('dateOfBirth')
                }
                transformed.append(transformed_character)
            except Exception as e:
                logger.error(f"Error en character {character.get('name', 'unknown')}: {e}")
                continue
        
        logger.info(f"Characters transformados: {len(transformed)}")
        return transformed
    
    def transform_all(self, raw_data: Dict[str, List[Dict]]) -> Dict[str, List[Dict]]:
        """Transforma los datos crudos a formato final."""
        logger.info("Transformando datos...")
        
        transformed_data = {
            'characters': self.transform_characters(raw_data.get('characters', []))
        }
        
        logger.info("Transformación completada")
        return transformed_data


if __name__ == "__main__":
    import json
    
    # Leer datos extraídos
    data_dir = os.getenv('DATA_DIR', '/app/data')
    input_file = os.path.join(data_dir, '1.raw_data.json')
    
    if not os.path.exists(input_file):
        logger.error(f"No se encontró {input_file}")
        exit(1)
    
    with open(input_file, 'r') as f:
        raw_data = json.load(f)
    
    # Transformar
    transformer = HPTransformer()
    transformed_data = transformer.transform_all(raw_data)
    
    # Guardar datos transformados
    output_file = os.path.join(data_dir, '2.transformed_data.json')
    with open(output_file, 'w') as f:
        json.dump(transformed_data, f, indent=2)
    
    logger.info(f"Datos transformados guardados en {output_file}")

# Transformación de datos y carga en MongoDB

import logging
import sys
import os
from typing import Dict, List, Optional

# Agregar ruta para importar extract
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '1.extract'))

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class HPTransformer:
    def __init__(
        self,
        mongo_connection_string: Optional[str] = None,
        mongo_database_name: str = "harry_potter",
        load_to_mongo: bool = True
    ):
        self.load_to_mongo = load_to_mongo
        if load_to_mongo:
            from load import MongoLoader
            self.loader = MongoLoader(
                connection_string=mongo_connection_string or "mongodb://localhost:27017/",
                database_name=mongo_database_name
            )
        else:
            self.loader = None
    
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
                        'wood': wand.get('wood'),  # madera
                        'core': wand.get('core'),  # núcleo
                        'length': self._parse_numeric(wand.get('length'))  # longitud
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
    
    def transform_all(self, raw_data: Dict[str, List[Dict]], load_to_mongo: Optional[bool] = None) -> Dict[str, List[Dict]]:
        logger.info("Transformando datos...")
        
        transformed_data = {
            'characters': self.transform_characters(raw_data.get('characters', []))
        }
        
        logger.info("Transformación completada")
        
        # cargar en mongo si está habilitado
        should_load = load_to_mongo if load_to_mongo is not None else self.load_to_mongo
        if should_load and self.loader:
            logger.info("Cargando en MongoDB...")
            load_results = self.loader.load_all(transformed_data, replace=True)
            logger.info(f"Carga completada: {load_results}")
        
        return transformed_data


if __name__ == "__main__":
    from extract import HPExtractorBase
    
    extractor = HPExtractorBase()
    raw_data = extractor.extract_all()
    
    transformer = HPTransformer()
    transformed_data = transformer.transform_all(raw_data)
    
    print(f"\nResumen:")
    print(f"  Characters: {len(transformed_data['characters'])}")

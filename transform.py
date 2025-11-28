"""
Módulo de Transformación (Transform) para el ETL de Star Wars
Transforma los datos extraídos de SWAPI en un formato más limpio y estructurado
"""

import logging
from typing import Dict, List, Optional
from datetime import datetime

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SWAPITransformer:
    """Clase para transformar datos de SWAPI"""
    
    def __init__(self):
        """Inicializa el transformador"""
        pass
    
    def _extract_id_from_url(self, url: str) -> Optional[int]:
        """
        Extrae el ID de una URL de SWAPI
        
        Args:
            url: URL de SWAPI (ej: "https://swapi.dev/api/people/1/")
            
        Returns:
            ID extraído o None si no se puede extraer
        """
        if not url:
            return None
        try:
            # La URL tiene formato: https://swapi.dev/api/resource/id/
            parts = url.rstrip('/').split('/')
            return int(parts[-1])
        except (ValueError, IndexError):
            return None
    
    def _parse_numeric(self, value: str) -> Optional[float]:
        """
        Convierte un string numérico a float
        
        Args:
            value: String que representa un número
            
        Returns:
            Float o None si no se puede convertir
        """
        if not value or value.lower() in ['unknown', 'n/a', 'none']:
            return None
        try:
            # Remover comas y convertir
            cleaned = value.replace(',', '')
            return float(cleaned)
        except (ValueError, AttributeError):
            return None
    
    def transform_people(self, people_data: List[Dict]) -> List[Dict]:
        """
        Transforma los datos de personas
        
        Args:
            people_data: Lista de diccionarios con datos de personas
            
        Returns:
            Lista de diccionarios transformados
        """
        logger.info(f"Transformando {len(people_data)} registros de people...")
        
        transformed = []
        for person in people_data:
            try:
                transformed_person = {
                    'id': self._extract_id_from_url(person.get('url')),
                    'name': person.get('name'),
                    'height': self._parse_numeric(person.get('height')),
                    'mass': self._parse_numeric(person.get('mass')),
                    'hair_color': person.get('hair_color'),
                    'skin_color': person.get('skin_color'),
                    'eye_color': person.get('eye_color'),
                    'birth_year': person.get('birth_year'),
                    'gender': person.get('gender'),
                    'homeworld_id': self._extract_id_from_url(person.get('homeworld')),
                    'film_ids': [self._extract_id_from_url(f) for f in person.get('films', [])],
                    'species_ids': [self._extract_id_from_url(s) for s in person.get('species', [])],
                    'vehicle_ids': [self._extract_id_from_url(v) for v in person.get('vehicles', [])],
                    'starship_ids': [self._extract_id_from_url(s) for s in person.get('starships', [])],
                    'created': person.get('created'),
                    'edited': person.get('edited'),
                    'url': person.get('url')
                }
                transformed.append(transformed_person)
            except Exception as e:
                logger.error(f"Error transformando person {person.get('name', 'unknown')}: {e}")
                continue
        
        logger.info(f"Transformación de people completada: {len(transformed)} registros")
        return transformed
    
    def transform_planets(self, planets_data: List[Dict]) -> List[Dict]:
        """
        Transforma los datos de planetas
        
        Args:
            planets_data: Lista de diccionarios con datos de planetas
            
        Returns:
            Lista de diccionarios transformados
        """
        logger.info(f"Transformando {len(planets_data)} registros de planets...")
        
        transformed = []
        for planet in planets_data:
            try:
                transformed_planet = {
                    'id': self._extract_id_from_url(planet.get('url')),
                    'name': planet.get('name'),
                    'rotation_period': self._parse_numeric(planet.get('rotation_period')),
                    'orbital_period': self._parse_numeric(planet.get('orbital_period')),
                    'diameter': self._parse_numeric(planet.get('diameter')),
                    'climate': planet.get('climate'),
                    'gravity': planet.get('gravity'),
                    'terrain': planet.get('terrain'),
                    'surface_water': self._parse_numeric(planet.get('surface_water')),
                    'population': self._parse_numeric(planet.get('population')),
                    'resident_ids': [self._extract_id_from_url(r) for r in planet.get('residents', [])],
                    'film_ids': [self._extract_id_from_url(f) for f in planet.get('films', [])],
                    'created': planet.get('created'),
                    'edited': planet.get('edited'),
                    'url': planet.get('url')
                }
                transformed.append(transformed_planet)
            except Exception as e:
                logger.error(f"Error transformando planet {planet.get('name', 'unknown')}: {e}")
                continue
        
        logger.info(f"Transformación de planets completada: {len(transformed)} registros")
        return transformed
    
    def transform_starships(self, starships_data: List[Dict]) -> List[Dict]:
        """
        Transforma los datos de naves espaciales
        
        Args:
            starships_data: Lista de diccionarios con datos de naves espaciales
            
        Returns:
            Lista de diccionarios transformados
        """
        logger.info(f"Transformando {len(starships_data)} registros de starships...")
        
        transformed = []
        for starship in starships_data:
            try:
                transformed_starship = {
                    'id': self._extract_id_from_url(starship.get('url')),
                    'name': starship.get('name'),
                    'model': starship.get('model'),
                    'manufacturer': starship.get('manufacturer'),
                    'cost_in_credits': self._parse_numeric(starship.get('cost_in_credits')),
                    'length': self._parse_numeric(starship.get('length')),
                    'max_atmosphering_speed': self._parse_numeric(starship.get('max_atmosphering_speed')),
                    'crew': starship.get('crew'),  # Puede ser un rango como "1-3"
                    'passengers': self._parse_numeric(starship.get('passengers')),
                    'cargo_capacity': self._parse_numeric(starship.get('cargo_capacity')),
                    'consumables': starship.get('consumables'),
                    'hyperdrive_rating': self._parse_numeric(starship.get('hyperdrive_rating')),
                    'MGLT': self._parse_numeric(starship.get('MGLT')),
                    'starship_class': starship.get('starship_class'),
                    'pilot_ids': [self._extract_id_from_url(p) for p in starship.get('pilots', [])],
                    'film_ids': [self._extract_id_from_url(f) for f in starship.get('films', [])],
                    'created': starship.get('created'),
                    'edited': starship.get('edited'),
                    'url': starship.get('url')
                }
                transformed.append(transformed_starship)
            except Exception as e:
                logger.error(f"Error transformando starship {starship.get('name', 'unknown')}: {e}")
                continue
        
        logger.info(f"Transformación de starships completada: {len(transformed)} registros")
        return transformed
    
    def transform_all(self, raw_data: Dict[str, List[Dict]]) -> Dict[str, List[Dict]]:
        """
        Transforma todos los datos extraídos
        
        Args:
            raw_data: Diccionario con las claves 'people', 'planets', 'starships'
            
        Returns:
            Diccionario con los datos transformados
        """
        logger.info("Iniciando transformación de datos...")
        
        transformed_data = {
            'people': self.transform_people(raw_data.get('people', [])),
            'planets': self.transform_planets(raw_data.get('planets', [])),
            'starships': self.transform_starships(raw_data.get('starships', []))
        }
        
        logger.info("Transformación completada")
        return transformed_data


if __name__ == "__main__":
    # Ejemplo de uso (requiere datos extraídos primero)
    from extract import SWAPIExtractorBase
    
    # Extraer datos
    extractor = SWAPIExtractorBase()
    raw_data = extractor.extract_all()
    
    # Transformar datos
    transformer = SWAPITransformer()
    transformed_data = transformer.transform_all(raw_data)
    
    print(f"\nResumen de transformación:")
    print(f"  - People: {len(transformed_data['people'])} registros")
    print(f"  - Planets: {len(transformed_data['planets'])} registros")
    print(f"  - Starships: {len(transformed_data['starships'])} registros")
    
    # Mostrar un ejemplo de cada tipo
    if transformed_data['people']:
        print(f"\nEjemplo de person transformado:")
        import json
        print(json.dumps(transformed_data['people'][0], indent=2))
    
    if transformed_data['planets']:
        print(f"\nEjemplo de planet transformado:")
        import json
        print(json.dumps(transformed_data['planets'][0], indent=2))
    
    if transformed_data['starships']:
        print(f"\nEjemplo de starship transformado:")
        import json
        print(json.dumps(transformed_data['starships'][0], indent=2))


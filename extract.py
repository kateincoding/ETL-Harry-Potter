"""
Módulo de Extracción (Extract) para el ETL de Star Wars
Extrae datos de la API SWAPI (https://swapi.dev/)
"""

import requests
import time
from typing import Dict, List, Optional
import logging

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

BASE_URL = "https://swapi.dev/api"


class SWAPIExtractorBase:
    """Clase base para extraer datos de la API SWAPI"""
    
    def __init__(self, base_url: str = BASE_URL, delay: float = 0.1):
        """
        Inicializa la clase extractor base
        
        Args:
            base_url: URL base de la API SWAPI
            delay: Tiempo de espera entre requests (en segundos) para evitar rate limiting
        """
        self.base_url = base_url
        self.delay = delay
        self.session = requests.Session()
    
    def _make_request(self, url: str) -> Optional[Dict]:
        """
        Realiza una petición HTTP a la API
        
        Args:
            url: URL completa a la que hacer la petición
            
        Returns:
            Diccionario con la respuesta JSON o None si hay error
        """
        try:
            time.sleep(self.delay)  # Evitar rate limiting
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Error al hacer request a {url}: {e}")
            return None
    
    def _fetch_all_pages(self, endpoint: str) -> List[Dict]:
        """
        Extrae todos los resultados de un endpoint paginado
        
        Args:
            endpoint: Endpoint de la API (ej: 'people', 'planets', 'starships')
            
        Returns:
            Lista con todos los resultados
        """
        all_results = []
        url = f"{self.base_url}/{endpoint}/"
        
        logger.info(f"Extrayendo datos de {endpoint}...")
        
        while url:
            data = self._make_request(url)
            if not data:
                break
                
            results = data.get('results', [])
            all_results.extend(results)
            logger.info(f"  - Página procesada: {len(results)} registros")
            
            # Obtener la siguiente página
            url = data.get('next')
        
        logger.info(f"Total de {endpoint} extraídos: {len(all_results)}")
        return all_results
    
    def extract_all(self) -> Dict[str, List[Dict]]:
        """
        Extrae todos los datos de people, planets y starships
        
        Returns:
            Diccionario con las claves 'people', 'planets', 'starships'
        """
        logger.info("Iniciando extracción de datos de SWAPI...")
        
        people_extractor = PeopleExtractor(self.base_url, self.delay)
        planets_extractor = PlanetsExtractor(self.base_url, self.delay)
        starships_extractor = StarshipsExtractor(self.base_url, self.delay)
        
        data = {
            'people': people_extractor.extract(),
            'planets': planets_extractor.extract(),
            'starships': starships_extractor.extract()
        }
        
        logger.info("Extracción completada")
        return data


class PeopleExtractor(SWAPIExtractorBase):
    """Clase para extraer datos de personas (people)"""
    
    def extract(self) -> List[Dict]:
        """
        Extrae todos los datos de personas
        
        Returns:
            Lista de diccionarios con datos de personas
        """
        return self._fetch_all_pages('people')


class PlanetsExtractor(SWAPIExtractorBase):
    """Clase para extraer datos de planetas (planets)"""
    
    def extract(self) -> List[Dict]:
        """
        Extrae todos los datos de planetas
        
        Returns:
            Lista de diccionarios con datos de planetas
        """
        return self._fetch_all_pages('planets')


class StarshipsExtractor(SWAPIExtractorBase):
    """Clase para extraer datos de naves espaciales (starships)"""
    
    def extract(self) -> List[Dict]:
        """
        Extrae todos los datos de naves espaciales
        
        Returns:
            Lista de diccionarios con datos de naves espaciales
        """
        return self._fetch_all_pages('starships')


if __name__ == "__main__":
    # Ejemplo de uso con la clase base
    extractor = SWAPIExtractorBase()
    data = extractor.extract_all()
    
    print(f"\nResumen de extracción:")
    print(f" Personas: {len(data['people'])} registros")
    print(f" Planetas: {len(data['planets'])} registros")
    print(f" Naves espaciales: {len(data['starships'])} registros")
    
    # Ejemplo de uso con clases específicas
    print("\n--- Ejemplo con clases específicas ---")
    people_extractor = PeopleExtractor()
    people_data = people_extractor.extract()
    print(f"Personas extraídas: {len(people_data)} registros")
    
    planets_extractor = PlanetsExtractor()
    planets_data = planets_extractor.extract()
    print(f"Planetas extraídos: {len(planets_data)} registros")
    
    starships_extractor = StarshipsExtractor()
    starships_data = starships_extractor.extract()
    print(f"Naves espaciales extraídas: {len(starships_data)} registros")


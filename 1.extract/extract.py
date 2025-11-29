# Extracción de datos de SWAPI

import requests
import time
from typing import Dict, List, Optional
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

BASE_URL = "https://swapi.dev/api"


class SWAPIExtractorBase:
    """Clase base para extraer datos de la API SWAPI"""
    
    def __init__(self, base_url: str = BASE_URL, delay: float = 0.1):
        """Inicializa la clase base para extraer datos de la API SWAPI"""
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
            time.sleep(self.delay)  # evitamos rate limiting
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Error en request a {url}: {e}")
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
        
        logger.info(f"Extrayendo {endpoint}...")
        
        while url:
            data = self._make_request(url)
            if not data:
                break
                
            results = data.get('results', [])
            all_results.extend(results)
            logger.info(f"  Página: {len(results)} registros")
            
            url = data.get('next')
        
        logger.info(f"Total {endpoint}: {len(all_results)}")
        return all_results
    
    def extract_all(self) -> Dict[str, List[Dict]]:
        """
        Extrae todos los datos de la API
        
        Returns:
            Diccionario con los datos extraídos
            como people, planets, starships
        """
        logger.info("Iniciando extracción...")
        
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
    extractor = SWAPIExtractorBase()
    data = extractor.extract_all()
    
    print(f"\nResumen:")
    print(f" Personas: {len(data['people'])}")
    print(f" Planetas: {len(data['planets'])}")
    print(f" Naves: {len(data['starships'])}")

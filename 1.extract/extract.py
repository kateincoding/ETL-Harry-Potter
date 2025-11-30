# Extracción de datos de Harry Potter API

import requests
import time
from typing import Dict, List, Optional
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

BASE_URL = "https://hp-api.onrender.com/api"


class HPExtractorBase:
    """Clase base para extraer datos de la API de Harry Potter"""
    
    def __init__(self, base_url: str = BASE_URL):
        """Inicializa la clase base para extraer datos de la API"""
        self.base_url = base_url
        self.session = requests.Session()
    
    def _make_request(self, url: str) -> Optional[List[Dict]]:
        """
        Realiza una request a la API de Harry Potter
        Argumentos: BASE_URL (harry potter api)
        Retorna: Lista con la respuesta JSON o None si hay error
        """
        try:
            time.sleep(1)  # por prevencion ponemos un tiempo de espera por los limites de la API
            response = self.session.get(url, timeout=15)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Error en request a {url}: {e}")
            return None
    
    def extract_characters(self) -> List[Dict]:
        """
        Extrae todos los personajes de Harry Potter
        
        Retorna: Lista de diccionarios con datos de personajes 
        """
        url = f"{self.base_url}/characters"
        logger.info("Extrayendo personajes de Harry Potter...")
        
        data = self._make_request(url)
        if not data:
            logger.error("No se pudieron extraer los datos")
            return []
        
        logger.info(f"Total personajes extraídos: {len(data)}")
        return data
    
    def extract_all(self) -> Dict[str, List[Dict]]:
        """
        Extrae todos los datos de la API
        Returns: Diccionario con los datos extraídos (characters)
        """
        logger.info("Iniciando extracción...")
        
        data = {
            'characters': self.extract_characters()
        }
        
        logger.info("Extracción completada")
        return data
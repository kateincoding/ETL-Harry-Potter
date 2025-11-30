"""
ETL Load de Harry Potter para MongoDB
"""

import logging
from typing import Dict, List, Optional
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.errors import PyMongoError

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class LoadHpToMongo:
    """Clase para cargar datos transformados en MongoDB"""
    
    def __init__(
        self,
        url_conexion: str = "mongodb://localhost:27017/",
        database_name: str = "harry_potter"
    ):
        """
        Inicializa
        """
        self.url_conexion = url_conexion
        self.database_name = database_name
        self.client: Optional[MongoClient] = None
        self.db: Optional[Database] = None
        
    def connect(self) -> bool:
        """
        Establece conexión con MongoDB
        
        Returns:
            True si la conexión fue exitosa, False en caso contrario
        """
        try:
            self.client = MongoClient(self.url_conexion)
            self.db = self.client[self.database_name]
            # Verificar conexión
            # self.client.admin.command('ping')
            logger.info(f"Conectado a MongoDB")
            return True
        except PyMongoError as e:
            logger.error(f"Error al conectar con MongoDB: {e}")
            return False
    
    def disconnect(self):
        """Cierra la conexión con MongoDB"""
        if self.client:
            self.client.close()
            logger.info("Conexión a MongoDB cerrada")
    
    def _get_collection(self, collection_name: str) -> Optional[Collection]:
        """
        Obtiene una colección de MongoDB
        Return: Objeto Collection o None si no hay conexión
        """
        if not self.db:
            logger.error("No hay conexión a la base de datos")
            return None
        return self.db[collection_name]
    
    def load_characters(self, characters_data: List[Dict], replace: bool = True) -> int:
        """
        Carga datos de personajes en MongoDB, si replace es True, reemplaza
        Retorna n de documentos insertados/actualizados
        """
        collection = self._get_collection('characters')
        if not collection:
            return 0
        
        try:
            if replace:
                # Eliminar todos los documentos existentes
                collection.delete_many({})
                logger.info("Documentos existentes eliminados")
            
            # Insertar documentos usando upsert basado en el campo 'id'
            inserted_count = 0
            for character in characters_data:
                try:
                    result = collection.replace_one(
                        {'id': character.get('id')},
                        character,
                        upsert=True
                    )
                    if result.upserted_id or result.modified_count > 0:
                        inserted_count += 1
                except Exception as e:
                    logger.error(f"Error insertando character {character.get('name', 'unknown')}: {e}")
                    continue
            
            logger.info(f"Characters cargados: {inserted_count} documentos")
            return inserted_count
        except PyMongoError as e:
            logger.error(f"Error al cargar characters en MongoDB: {e}")
            return 0
    
    def load_all(self, transformed_data: Dict[str, List[Dict]], replace: bool = True) -> Dict[str, int]:
        """
        Carga todos los datos transformados en MongoDB
        Retorna dict con el número de documentos cargados por colección
        """
        if not self.connect():
            logger.error("No se pudo conectar a MongoDB")
            return {'characters': 0}
        
        try:
            logger.info("=========Iniciando carga de datos en MongoDB=========")
            results = {
                'characters': self.load_characters(transformed_data.get('characters', []), replace)
            }
            
            logger.info("Carga completada")
            print("resultado", len(results))
            return results
        finally:
            self.disconnect()


if __name__ == "__main__":
    import json
    import os
    data_dir = os.getenv('DATA_DIR', './data')
    input_file = os.path.join(data_dir, 'transformed_data.json')
    
    if os.path.exists(input_file):
        with open(input_file, 'r') as f:
            transformed_data = json.load(f)
        
        # Cargar en MongoDB
        loader = LoadHpToMongo()
        results = loader.load_all(transformed_data)
        
        print(f"\nResumen de carga en MongoDB:")
        print(f"  - Characters: {results['characters']} documentos")
    else:
        print(f"Error: No se encontró {input_file}")
        exit(1)


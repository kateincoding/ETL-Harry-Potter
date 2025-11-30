# Setup de MongoDB - crea colecciones e índices

import logging
from pymongo import MongoClient, ASCENDING
from pymongo.errors import PyMongoError

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MongoSetup:
    """Clase para configurar las colecciones de MongoDB"""
    
    def __init__(
        self,
        connection_string: str = "mongodb://localhost:27017/",
        database_name: str = "harry_potter"
    ):
        self.connection_string = connection_string
        self.database_name = database_name
        self.client: MongoClient = None
        self.db = None
    
    def connect(self) -> bool:
        """
        Establece conexión con MongoDB
        
        Returns:
            True si la conexión fue exitosa, False en caso contrario
        """
        try:
            self.client = MongoClient(self.connection_string)
            self.db = self.client[self.database_name]
            # Verificar conexión
            self.client.admin.command('ping')
            logger.info(f"Conectado a MongoDB: {self.database_name}")
            return True
        except PyMongoError as e:
            logger.error(f"Error conectando: {e}")
            return False
    
    def disconnect(self):
        """Cierra la conexión con MongoDB"""
        if self.client:
            self.client.close()
            logger.info("Desconectado")
    
    def create_hp_collections(self):
        """Crea las colecciones si no existen"""
        if not self.db:
            logger.error("No hay conexión")
            return False
        
        collections = ['characters']
        
        for collection in collections:
            if collection not in self.db.list_collection_names():
                self.db.create_collection(collection)
                logger.info(f"Colección '{collection}' creada")
            else:
                logger.info(f"Colección '{collection}' ya existe")
        
        return True
    
    def create_indexes(self):
        """Crea índices para mejorar el rendimiento de las consultas"""
        if not self.db:
            logger.error("No hay conexión")
            return False
        
        try:
            # Índices para characters
            characters_collection = self.db['characters']
            characters_collection.create_index([('id', ASCENDING)], unique=True)
            characters_collection.create_index([('name', ASCENDING)])
            characters_collection.create_index([('house', ASCENDING)])
            characters_collection.create_index([('ancestry', ASCENDING)])
            characters_collection.create_index([('wand_core', ASCENDING)])
            characters_collection.create_index([('year_of_birth', ASCENDING)])
            logger.info("Índices creados para 'characters'")
            
            return True
        except PyMongoError as e:
            logger.error(f"Error creando índices: {e}")
            return False
    
    def setup_all(self):
        """
        Ejecuta todo el setup: crea colecciones e índices
        
        Returns:
            True si todo fue exitoso, False en caso contrario
        """
        if not self.connect():
            return False
        
        try:
            logger.info("Iniciando setup...")
            
            if not self.create_hp_collections():
                return False
            
            if not self.create_indexes():
                return False
            
            logger.info("Setup completado")
            return True
        except Exception as e:
            logger.error(f"Error en setup: {e}")
            return False
        finally:
            self.disconnect()
    
    def drop_collections(self):
        """
        Elimina todas las colecciones (¡CUIDADO! Esto borra todos los datos)
        
        Returns:
            True si fue exitoso, False en caso contrario
        """
        if not self.db:
            logger.error("No hay conexión")
            return False
        
        try:
            collections = ['characters']
            for collection in collections:
                if collection in self.db.list_collection_names():
                    self.db.drop_collection(collection)
                    logger.warning(f"Colección '{collection}' eliminada")
            
            logger.warning("Todas las colecciones eliminadas")
            return True
        except PyMongoError as e:
            logger.error(f"Error eliminando: {e}")
            return False


if __name__ == "__main__":
    import sys
    
    setup = MongoSetup()
    
    if len(sys.argv) > 1 and sys.argv[1] == '--drop':
        if setup.connect():
            response = input("¿Eliminar todas las colecciones? (yes/no): ")
            if response.lower() == 'yes':
                setup.drop_collections()
            setup.disconnect()
    else:
        setup.setup_all()
        print("FIN: characters creados con : id (único), name, house, gender, ancestry, year_of_birth")

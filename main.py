# ETL principal de Harry Potter

import logging
import sys
sys.path.insert(0, '1.extract')
from extract import HPExtractorBase
from transform import HPTransformer

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def run_etl(mongo_url_conexion: str = None, load_to_mongo: bool = True):
    """
    Ejecuta el proceso ETL completo (Extract, Transform y Load)
    
    Args:
        mongo_url_conexion: String de conexión a MongoDB (None para usar default)
        load_to_mongo: Si True, carga los datos en MongoDB después de transformarlos
    """
    logger.info("=" * 60)
    logger.info("Iniciando ETL de Harry Potter")
    logger.info("=" * 60)
    
    # EXTRACT
    logger.info("\n[EXTRACT] Extrayendo datos de HP API...")
    extractor = HPExtractorBase()
    raw_data = extractor.extract_all()
    
    logger.info(f"\nResumen extracción:")
    logger.info(f"  Characters: {len(raw_data['characters'])}")
    
    # TRANSFORM & LOAD
    logger.info("\n[TRANSFORM & LOAD] Transformando y cargando en MongoDB...")
    transformer = HPTransformer(
        mongo_url_conexion=mongo_url_conexion,
        load_to_mongo=load_to_mongo
    )
    transformed_data = transformer.transform_all(raw_data, load_to_mongo=load_to_mongo)
    
    logger.info(f"\nResumen transformación:")
    logger.info(f"  Characters: {len(transformed_data['characters'])}")
    
    if load_to_mongo:
        logger.info("\n✓ Datos cargados en MongoDB")
    
    logger.info("\n" + "=" * 60)
    logger.info("ETL completado")
    logger.info("=" * 60)
    
    return transformed_data


if __name__ == "__main__":
    transformed_data = run_etl()
    
    print("\n" + "=" * 60)
    print("EJEMPLOS")
    print("=" * 60)
    
    if transformed_data['characters']:
        print("\n--- Character ---")
        character = transformed_data['characters'][0]
        print(f"ID: {character['id']}")
        print(f"Nombre: {character['name']}")
        print(f"Casa: {character['house']}")
        print(f"Año de nacimiento: {character['year_of_birth']}")
        print(f"Pureza de sangre: {character['ancestry']}")
        print(f"Género: {character['gender']}")
        if character.get('wand'):
            wand = character['wand']
            print(f"Varita - Madera: {wand.get('wood')}, Núcleo: {wand.get('core')}, Longitud: {wand.get('length')}")

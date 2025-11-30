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
    logger.info("Iniciando ETL de Harry Potter")
    
    # EXTRACT
    logger.info("\n[EXTRACT] Extrayendo datos de HP API...")
    extractor = HPExtractorBase()
    raw_data = extractor.extract_all()
    
    logger.info(f"\nResumen extracción:")
    logger.info(f"  Characters: {len(raw_data['characters'])}")
    
    transformer = HPTransformer(
        mongo_url_conexion=mongo_url_conexion,
        load_to_mongo=load_to_mongo
    )
    transformed_data = transformer.transform_all(raw_data, load_to_mongo=load_to_mongo)
    
    logger.info(f"\nTerminada la transformación:")
    logger.info(f"  len de Characters: {len(transformed_data['characters'])}")
    
    if load_to_mongo:
        logger.info("\n✓ Fase 3: Carga de datos en MongoDB completado")
    
    logger.info("ETL completado")
    
    return transformed_data


if __name__ == "__main__":
    transformed_data = run_etl()
    
    if transformed_data['characters']:
        character = transformed_data['characters'][0]
        if character.get('wand'):
            wand = character['wand']
            print(f"Varita - Madera: {wand.get('wood')}, Núcleo: {wand.get('core')}, Longitud: {wand.get('length')}")



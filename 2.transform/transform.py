# Transformación de datos y carga en MongoDB

import json
import logging
import os
from typing import Dict, List, Optional, Tuple
from statistics import mean, median, stdev
import matplotlib.pyplot as plt
import seaborn as sns

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class HPTransformer:
    def __init__(self):
        """Inicializa el transformador."""
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
                wizard = bool(character.get('wizard'))
                # Filtramos la data para solo tener a los wizard
                if wizard is False:
                    continue
                # Extraer información de la varita
                wand = character.get('wand', {})
                year_of_birth = self._parse_numeric(character.get('yearOfBirth'))
                
                transformed_character = {
                    'id': character.get('id'),
                    'name': character.get('name'),
                    'alternate_names': character.get('alternate_names', []),
                    'house': character.get('house'),
                    'year_of_birth': int(year_of_birth) if year_of_birth is not None else None, #transofrmamos en dos pasos para evitar errores
                    'ancestry': character.get('ancestry'),  # pureza de sangre
                    'gender': character.get('gender'),  # male/female
                    'species': character.get('species'),
                    'wizard': character.get('wizard'),
                    'wand_wood': wand.get('wood'),
                    'wand_core': wand.get('core'), # centro de la varita
                    'wand_length': self._parse_numeric(wand.get('length')),
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


class DescriptiveAnalysis:
    """Clase para análisis descriptivo y selección de variables sin dependencias externas"""
    
    def __init__(self, transformed_data: List[Dict]):
        """
        Inicializa el análisis descriptivo
        Args: transformed_data: Lista de diccionarios con datos transformados de personajes
        """
        self.data = transformed_data
        logger.info(f"Datos cargados para análisis: {len(self.data)} registros")
    
    def _get_numeric_columns(self) -> List[str]:
        """Identifica columnas numéricas (excluyendo 'id')"""
        if not self.data:
            return []
        
        numeric_cols = set()
        for record in self.data:
            for key, value in record.items():
                if key != 'id' and isinstance(value, (int, float)) and not isinstance(value, bool):
                    numeric_cols.add(key)

        print("==============numeric cols:", sorted(list(numeric_cols)))
        return sorted(list(numeric_cols))
    
    def _extract_numeric_values(self, column: str) -> List[float]:
        """Extrae valores numéricos válidos de una columna"""
        values = []
        for record in self.data:
            value = record.get(column)
            if value is not None and isinstance(value, (int, float)) and not isinstance(value, bool):
                values.append(float(value))
        return sorted(values)
    
    def statistical_summary(self) -> Dict[str, Dict]:
        """
        Calcula estadísticas descriptivas (media, mediana, Q1, Q3) para variables numéricas
        Returns: Diccionario con estadísticas por variable numérica
        """
        logger.info("Calculando estadísticas descriptivas...")
        
        numeric_cols = self._get_numeric_columns()
        stats = {}
        
        for col in numeric_cols:
            values = self._extract_numeric_values(col)
            
            if len(values) > 0:
                n = len(values)
                
                q1_idx = n // 4
                median_idx = n // 2
                q3_idx = (3 * n) // 4
                
                stats[col] = {
                    'mean': mean(values),
                    'median': median(values),
                    'q1': values[q1_idx],
                    'q3': values[q3_idx],
                    'std': stdev(values) if len(values) > 1 else 0.0,
                    'min': min(values),
                    'max': max(values),
                    'count': n
                }
        
        logger.info(f"Estadísticas calculadas para {len(stats)} variables")
        return stats
    
    def select_best_features(self, dependent_variable: str = 'house', top_n: int = 4) -> List[Tuple[str, float]]:
        """
        Selecciona las mejores variables numéricas basadas en correlación con la variable dependiente "house"
        Args: dependent_variable y top_n: Número de mejores variables
        Returns: Lista de tuplas para que no se repita (nombre_variable, correlación) ordenadas por correlación descendente
        """
        logger.info(f"Seleccionando {top_n} mejores variables con respecto a '{dependent_variable}'...")
        
        # Extraer valores de la variable dependiente y codificarlos
        target_values = []
        target_mapping = {}
        next_code = 0
        
        for record in self.data:
            target_val = record.get(dependent_variable)
            if target_val is not None:
                if target_val not in target_mapping:
                    target_mapping[target_val] = next_code
                    next_code += 1
                target_values.append(target_mapping[target_val])
            else:
                target_values.append(None)
        
        # correlación con cada variable numérica
        numeric_cols = self._get_numeric_columns()
        correlations = []
        
        for col in numeric_cols:
            # Obtener pares de valores válidos
            pairs = []
            for i, record in enumerate(self.data):
                try:
                    val = record.get(col)
                    if i < len(target_values) and target_values[i] is not None:
                        if val is not None and isinstance(val, (int, float)):
                            pairs.append((float(val), target_values[i]))
                except:
                    continue
            
            if len(pairs) > 1:
                # Calcular correlación
                corr = self._pearson_correlation([p[0] for p in pairs], [p[1] for p in pairs])
                correlations.append((col, abs(corr)))
        
        # Ordenar por correlación descendente
        correlations.sort(key=lambda x: x[1], reverse=True)
        best_features = correlations[:top_n]
        
        logger.info(f"Mejores variables: {best_features}")
        return best_features
    
    def _pearson_correlation(self, x: List[float], y: List[float]) -> float:
        """Calcula correlación de Pearson entre dos listas"""
        if len(x) != len(y) or len(x) < 2:
            return 0.0
        
        n = len(x)
        mean_x = mean(x)
        mean_y = mean(y)
        
        numerator = sum((x[i] - mean_x) * (y[i] - mean_y) for i in range(n))
        denominator = (sum((x[i] - mean_x) ** 2 for i in range(n)) * 
                      sum((y[i] - mean_y) ** 2 for i in range(n))) ** 0.5
        
        if denominator == 0:
            return 0.0
        
        return numerator / denominator
    
    def get_correlation_matrix(self) -> Dict[str, Dict[str, float]]:
        """
        Calcula la matriz de correlación para todas las variables numéricas
        
        Returns:
            Diccionario con la matriz de correlación
        """
        logger.info("Calculando matriz de correlación...")
        
        numeric_cols = self._get_numeric_columns()
        corr_matrix = {}
        
        for col1 in numeric_cols:
            corr_matrix[col1] = {}
            values1 = self._extract_numeric_values(col1)
            
            for col2 in numeric_cols:
                if col1 == col2:
                    corr_matrix[col1][col2] = 1.0
                else:
                    values2 = self._extract_numeric_values(col2)
                    
                    # Emparejar valores válidos
                    pairs = []
                    for i, record in enumerate(self.data):
                        val1 = record.get(col1)
                        val2 = record.get(col2)
                        if (val1 is not None and val2 is not None and 
                            isinstance(val1, (int, float)) and isinstance(val2, (int, float))):
                            pairs.append((float(val1), float(val2)))
                    
                    if len(pairs) > 1:
                        x_vals = [p[0] for p in pairs]
                        y_vals = [p[1] for p in pairs]
                        corr = self._pearson_correlation(x_vals, y_vals)
                        corr_matrix[col1][col2] = round(corr, 4)
                    else:
                        corr_matrix[col1][col2] = 0.0
        
        return corr_matrix
    
    def generate_report(self, dependent_variable: str = 'house', top_n: int = 4) -> Dict:
        """
        Genera un reporte completo con análisis descriptivo y selección de variables
        
        Args:
            dependent_variable: Variable dependiente
            top_n: Número de mejores variables
            
        Returns:
            Diccionario con el reporte completo
        """
        logger.info("Generando reporte de análisis...")
        
        report = {
            'total_records': len(self.data),
            'total_columns': len(self.data[0]) if self.data else 0,
            'statistical_summary': self.statistical_summary(),
            'best_features': self.select_best_features(dependent_variable, top_n),
            'correlation_matrix': self.get_correlation_matrix()
        }
        
        logger.info("Reporte completado")
        return report
    
    def save_report(self, output_path: str, dependent_variable: str = 'house', top_n: int = 4) -> bool:
        """
        Genera y guarda el reporte en un archivo JSON
        
        Args:
            output_path: Ruta donde guardar el archivo del reporte
            dependent_variable: Variable dependiente
            top_n: Número de mejores variables
            
        Returns:
            True si se guardó exitosamente, False en caso contrario
        """
        try:
            report = self.generate_report(dependent_variable, top_n)
            
            with open(output_path, 'w') as f:
                json.dump(report, f, indent=2)
            
            logger.info(f"Reporte guardado en: {output_path}")
            return True
        except Exception as e:
            logger.error(f"Error al guardar reporte: {e}")
            return False
    
    def plot_bivariate(self, x_column: str, y_column: str, output_path: str):
        """
        Genera un gráfico bivariante entre una variable numérica y la variable dependiente categórica.
        argumentos:
            x_column: Variable numérica (ej: 'wand_length')
            y_column: Variable dependiente (ej: 'house')
            output_path: Ruta para guardar la imagen (.png)
        """
        try:
            # Filtrar datos válidos
            x_vals = []
            y_vals = []
            for record in self.data:
                x = record.get(x_column)
                y = record.get(y_column)
                if x is not None and isinstance(x, (int, float)) and y is not None:
                    x_vals.append(x)
                    y_vals.append(y)
            
            if not x_vals or not y_vals:
                logger.warning(f"No hay datos válidos para graficar {x_column} vs {y_column}")
                return
            
            # Crear DataFrame temporal
            import pandas as pd
            df = pd.DataFrame({x_column: x_vals, y_column: y_vals})
            
            # Crear gráfico
            plt.figure(figsize=(10, 6))
            sns.boxplot(x=y_column, y=x_column, data=df)
            sns.stripplot(x=y_column, y=x_column, data=df, color='black', alpha=0.3, jitter=True)
            
            plt.title(f"{x_column} vs {y_column}")
            plt.xticks(rotation=45)
            plt.tight_layout()
            
            # Guardar imagen
            plt.savefig(output_path)
            plt.close()
            logger.info(f"Gráfica guardada en {output_path}")
        except Exception as e:
            logger.error(f"Error generando gráfica {x_column} vs {y_column}: {e}")
    
    def plot_all_bivariates(self, dependent_variable: str = 'house', output_dir: str = './plots'):
        """Genera gráficas para todas las variables numéricas vs variable dependiente"""
        numeric_cols = self._get_numeric_columns()
        logger.info(f"numeric variables:  {numeric_cols}")
        for col in numeric_cols:
            self.plot_bivariate(col, dependent_variable, os.path.join(output_dir, f"{col}_vs_{dependent_variable}.png"))

    def generate_html_report(self, plots_dir: str, output_html: str):
        """
        Genera un HTML simple metricas como las gráficas bivariantes.
        Argumentos:
            plots_dir: Carpeta donde están las imágenes generadas
            output_html: Ruta del archivo HTML a generar
        """
        try:
            # Listar imágenes
            images = sorted([f for f in os.listdir(plots_dir) if f.lower().endswith(('.png', '.jpeg', '.jpg'))])
            if not images:
                logger.warning(f"No se encontraron imágenes en {plots_dir}")
                return

            html_content = "<html><head><title>Reporte de Gráficas</title></head><body>\n"
            html_content += "<h1>Reporte de Gráficas Bivariantes</h1>\n"

            for img in images:
                img_path = os.path.join(os.path.basename(plots_dir), img)
                html_content += f"<div style='margin-bottom: 30px;'>\n"
                html_content += f"<h3>{img.replace('_', ' ').replace('.png','')}</h3>\n"
                html_content += f"<p>Aqui observamos la relación entre estas dos variables: {img.replace('_', ' ').replace('.png', '')} en donde 'house' es la casa en donde pertenece el mago</p>\n"
                html_content += f"<img src='{img_path}' style='max-width: 800px; width: 100%;'>\n"
                html_content += "</div>\n"

            html_content += "</body></html>"

            os.makedirs(os.path.dirname(output_html), exist_ok=True)
            with open(output_html, 'w') as f:
                f.write(html_content)

            logger.info(f"HTML report generado en {output_html}")
        except Exception as e:
            logger.error(f"Error generando HTML: {e}")

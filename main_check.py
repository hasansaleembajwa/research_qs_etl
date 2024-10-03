import traceback
import pandas as pd
from src.extractors.sql_server_extractor import SQLServerExtractor
from src.transformers.question_transformer import ResearchQuestionTransformer
from src.loaders.snowflake_loader import SnowflakeLoader
from src.utils.logging_utils import setup_logger
import yaml
import traceback
import subprocess
import sys
import numpy as np

logger = setup_logger(__name__)

def main():
    
    # try:
    with open("config/snowflake_config.yaml", 'r') as config_file:
        snowflake_config = yaml.safe_load(config_file)

    extractor = SQLServerExtractor("config/sql_server_config.yaml")
    extraction_result = extractor.extract_all_data()

    logger.debug(f"Extraction result keys: {extraction_result.keys()}")
    logger.debug(f"Combined data shape: {extraction_result['combined_data'].shape}")
    logger.debug(f"Combined data columns: {extraction_result['combined_data'].columns.tolist()}")
    
    transformer = ResearchQuestionTransformer(extraction_result['combined_data'], extraction_result['metadata'])
    transformation_result = transformer.run_transformation()

    # Save results
    transformation_result['unified_data'].to_csv("unified_research_questions.csv", index=False)
    transformation_result['question_evolution'].to_csv("question_evolution.csv", index=False)
    transformation_result['answer_evolution'].to_csv("answer_evolution.csv", index=False)
    
    # except Exception as e:
    #     logger.error(f"An error occurred in main")
    
if __name__ == "__main__":
    main()
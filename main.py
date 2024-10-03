import yaml
import subprocess
from src.extractors.sql_server_extractor import SQLServerExtractor
from src.transformers.question_transformer import QuestionTransformer
from src.loaders.snowflake_loader import SnowflakeLoader
from src.utils.logging_utils import setup_logger

logger = setup_logger(__name__)



def load_config(file_path):
    with open(file_path, 'r') as file:
        return yaml.safe_load(file)

def run_dbt():
    try:
        result = subprocess.run(
            ['dbt', 'run', '--profiles-dir', 'dbt', '--project-dir', 'dbt'],
            check=True,
            capture_output=True,
            text=True
        )
        logger.info("dbt models executed successfully")
        logger.debug(result.stdout)
    except subprocess.CalledProcessError as e:
        logger.error(f"dbt execution failed: {e}")
        logger.error(e.stdout)
        logger.error(e.stderr)
        
def main():
    snowflake_config = load_config('config/snowflake_config.yaml')
    sql_server_config = load_config('config/sql_server_config.yaml')
    
    extractor = SQLServerExtractor("config/sql_server_config.yaml")
    transformer = QuestionTransformer()
    snowflake_loader = SnowflakeLoader(snowflake_config)
    
    for year in [2021, 2022, 2023]:
        data = extractor.extract_questions_with_options(year)
        if data is not None:
            print(f"Year {year}:")
            print(data.head())
            transformer.transform_questions(data, year)
            print("\n")
            
        # responses_data = extractor.extract_questions_with_options(year, 'Responses')
        # if responses_data is not None:
        #     print(f"Year {year}:")
        #     print(responses_data)
        #     print("\n")
            
    consolidated_data = transformer.get_consolidated_questions()
    consolidated_data.to_csv("consolidated_questions_data.csv", index=False)
    logger.info("Consolidated data saved to 'consolidated_questions_data.csv'")

    success, nrows = snowflake_loader.load_data(consolidated_data)
    if success:
        logger.info(f"Successfully loaded {nrows} rows to Snowflake")
    else:
        logger.error("Failed to load data to Snowflake")
        return

    try:
        subprocess.run(['dbt', 'run', '--profiles-dir', 'dbt', '--project-dir', 'dbt'], check=True)
        logger.info("dbt run completed successfully")
    except subprocess.CalledProcessError as e:
        logger.error(f"dbt execution failed: {e}")
    finally:
        snowflake_loader.close()

    logger.info("ETL process completed")
    
if __name__ == "__main__":
    main()


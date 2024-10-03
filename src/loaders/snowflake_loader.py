# File: src/loaders/snowflake_loader.py


import snowflake.connector # type: ignore
import pandas as pd
from src.utils.logging_utils import setup_logger
import json

logger = setup_logger(__name__)

class SnowflakeLoader:
    def __init__(self, config):
        self.config = config
        self.conn = None

    def connect(self):
        """Establish connection to Snowflake."""
        if self.conn is not None:
            return self.conn
        
        try:
            self.conn = snowflake.connector.connect(
                account=self.config['account'],
                user=self.config['user'],
                password=self.config['password'],
                warehouse=self.config['warehouse'],
                database=self.config['database'],
                schema=self.config['schema']
            )
            logger.info("Connected to Snowflake")
            return self.conn
        except Exception as e:
            logger.error(f"Error connecting to Snowflake: {e}")
            raise

    def load_data(self, df):
        """Load data into Snowflake table."""
        try:
            conn = self.connect()
            cursor = conn.cursor()

            # Create the table if it doesn't exist
            create_table_query = """
            CREATE TABLE IF NOT EXISTS consolidated_research_questions (
                question_id STRING,
                year NUMBER,
                question_text STRING,
                category STRING,
                is_active BOOLEAN,
                valid_from DATE,
                valid_to DATE,
                answer_set VARIANT
            )
            """
            cursor.execute(create_table_query)

            # Prepare data for insertion
            def prepare_answer_set(answer_set):
                return json.dumps(answer_set)

            df['answer_set'] = df['answer_set'].apply(prepare_answer_set)

            # Insert data
            insert_query = """
            INSERT INTO consolidated_research_questions 
            (question_id, year, question_text, category, is_active, valid_from, valid_to, answer_set)
            SELECT
                column1, column2, column3, column4, column5, 
                TO_DATE(column6), 
                CASE WHEN column7 = 'None' THEN NULL ELSE TO_DATE(column7) END,
                TO_VARIANT(PARSE_JSON(column8))
            FROM VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """

            data_to_insert = df.apply(lambda row: (
                row['question_id'],
                row['year'],
                row['question_text'],
                row['category'],
                row['is_active'],
                row['valid_from'],
                row['valid_to'] if pd.notna(row['valid_to']) else 'None',
                row['answer_set']
            ), axis=1).tolist()

            cursor.executemany(insert_query, data_to_insert)

            conn.commit()
            logger.info(f"Successfully loaded {len(df)} rows into Snowflake")
        except Exception as e:
            logger.error(f"Error loading data into Snowflake: {e}")
            raise
        finally:
            if cursor:
                cursor.close()

    def close_connection(self):
        """Close the Snowflake connection."""
        if self.conn:
            self.conn.close()
            self.conn = None
            logger.info("Snowflake connection closed")

# Example usage:
if __name__ == "__main__":
    from src.extractors.sql_server_extractor import SQLServerExtractor
    from src.transformers.question_transformer import QuestionTransformer
    import yaml

    with open("config/snowflake_config.yaml", 'r') as config_file:
        snowflake_config = yaml.safe_load(config_file)

    extractor = SQLServerExtractor("config/sql_server_config.yaml")
    transformer = QuestionTransformer()
    loader = SnowflakeLoader(snowflake_config)

    try:
        for year in [2021, 2022, 2023]:
            year_data = extractor.extract_questions_with_options(year)
            if year_data is not None:
                transformer.transform_questions(year_data, year)

        consolidated_data = transformer.get_consolidated_questions()
        loader.load_data(consolidated_data)
    except Exception as e:
        logger.error(f"An error occurred during the ETL process: {e}")
    finally:
        loader.close_connection()


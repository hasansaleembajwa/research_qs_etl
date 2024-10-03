import pandas as pd
import yaml
import urllib
import os
from dotenv import load_dotenv
from src.utils.logging_utils import setup_logger
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

logger = setup_logger(__name__)

class SQLServerExtractor:
    def __init__(self, config_path):
        load_dotenv()
        with open(config_path, 'r') as config_file:
            self.config = yaml.safe_load(config_file)
        self.engines = {}

    def _get_connection_string(self, server_config):
        username = os.getenv('SQL_SERVER_USERNAME')
        password = os.getenv('SQL_SERVER_PASSWORD')
        if not username or not password:
            raise ValueError("SQL Server username or password not set in environment variables")
        params = urllib.parse.quote_plus(f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server_config['host']};DATABASE={server_config['database']};UID={username};PWD={password};Connection Timeout=30")
        return f"mssql+pyodbc:///?odbc_connect={params}"

    def connect(self, year):
        """Establish connection to SQL Server for a specific year."""
        try:
            server_config = next(server for server in self.config['sql_servers'] if str(year) in server['database'])
            conn_str = self._get_connection_string(server_config)
            engine = create_engine(conn_str, connect_args={'timeout': 30})
            logger.info(f"Connected to SQL Server for year {year}")
            return engine
        except StopIteration:
            logger.error(f"No configuration found for year {year}")
            return None
        except SQLAlchemyError as e:
            logger.error(f"Error connecting to SQL Server for year {year}: {e}")
            logger.error(f"Connection string used: {conn_str}")
            return None

    def extract_questions_with_options(self, year):
        """Extract questions and their corresponding answer options for a specific year."""
        engine = self.connect(year)
        if not engine:
            return None

        try:
            query = text("""
            SELECT 
                q.QuestionID,
                q.QuestionText,
                qc.CategoryName,
                q.IsActive,
                q.QuestionOrder,
                ao.AnswerOptionID,
                ao.OptionText,
                ao.OptionOrder
            FROM Questions q
            LEFT JOIN QuestionCategories qc ON q.CategoryID = qc.CategoryID
            LEFT JOIN AnswerOptions ao ON q.QuestionID = ao.QuestionID
            ORDER BY q.QuestionOrder, ao.OptionOrder
            """)
            
            with engine.connect() as connection:
                df = pd.read_sql(query, connection)
            
            if df.empty:
                logger.warning(f"No data extracted for year {year}")
                return None

            # Group answer options for each question
            grouped = df.groupby(['QuestionID', 'QuestionText', 'CategoryName', 'IsActive', 'QuestionOrder'])
            questions = []
            for (qid, qtext, category, is_active, order), group in grouped:
                answer_options = group[['AnswerOptionID', 'OptionText', 'OptionOrder']].to_dict('records')
                questions.append({
                    'year': year,
                    'question_id': f"{year}_{qid}",
                    'question_text': qtext,
                    'category': category,
                    'is_active': is_active,
                    'question_order': order,
                    'answer_set': answer_options
                })

            result_df = pd.DataFrame(questions)
            logger.info(f"Extracted and processed {len(result_df)} questions from database for year {year}")
            return result_df
        except SQLAlchemyError as e:
            logger.error(f"Error extracting data for year {year}: {e}")
            return None
        finally:
            engine.dispose()

# Example usage
if __name__ == "__main__":
    extractor = SQLServerExtractor("config/sql_server_config.yaml")
    for year in [2021, 2022, 2023]:
        df = extractor.extract_questions_with_options(year)
        if df is not None:
            print(f"Year {year}:")
            print(df.head())
            print("\n")
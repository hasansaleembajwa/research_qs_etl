import traceback
import pandas as pd
from src.extractors.sql_server_extractor import SQLServerExtractor
from src.transformers.question_transformer import QuestionTransformer
from src.loaders.snowflake_loader import SnowflakeLoader
from src.utils.logging_utils import setup_logger
import yaml
import traceback
import subprocess
import sys
import numpy as np

logger = setup_logger(__name__)

def main():
    with open("config/snowflake_config.yaml", 'r') as config_file:
        snowflake_config = yaml.safe_load(config_file)

    extractor = SQLServerExtractor("config/sql_server_config.yaml")
    transformer = QuestionTransformer()
    loader = SnowflakeLoader(snowflake_config)

    for year in [2021, 2022, 2023]:
        try:
            logger.info(f"Extracting data for year {year}")
            data = extractor.extract_questions_with_options(year)
            
            if data is None or data.empty:
                logger.warning(f"No data extracted for year {year}")
                continue

            logger.info(f"Transforming data for year {year}")
            logger.info(f"Number of questions for year {year}: {len(data)}")
            transformer.transform_questions(data, year)
        except Exception as e:
            logger.error(f"An error occurred processing year {year}: {str(e)}")
            logger.error(traceback.format_exc())
            break

    try:
        consolidated_data = transformer.get_consolidated_questions()
        logger.info(f"Total consolidated questions: {len(consolidated_data)}")
        logger.info(f"Columns in consolidated data: {consolidated_data.columns}")
        
        # Save the consolidated data to a CSV file
        output_file = "consolidated_questions.csv"
        consolidated_data.to_csv(output_file, index=False)
        logger.info(f"Consolidated questions saved to {output_file}")

        # Print summary of the consolidated data
        print("\nSummary of consolidated data:")
        summary = consolidated_data[['year', 'question_id', 'question_text', 'is_active', 'valid_to', 'answer_set_version', 'text_changed']]
        summary['answer_set_changed'] = consolidated_data['previous_answer_set'].notna()
        print(summary.to_string())
        
        if not consolidated_data.empty:
            # Print summary of the consolidated data
            print("\nSummary of consolidated data:")
            summary = consolidated_data[['year', 'question_id', 'question_text', 'is_active', 'valid_to', 'answer_set_version', 'text_changed']]
            summary['answer_set_changed'] = consolidated_data['previous_answer_set'].notna()
            print(summary.to_string())

            # Print statistics
            print("\nStatistics:")
            print(f"Total questions: {len(consolidated_data)}")
            print(f"Active questions: {len(consolidated_data[consolidated_data['is_active']])}")
            print(f"Inactive questions: {len(consolidated_data[~consolidated_data['is_active']])}")
            print(f"New questions: {len(consolidated_data[consolidated_data['first_seen_year'].notna()])}")
            print(f"Questions with text changes: {len(consolidated_data[consolidated_data['text_changed']])}")
            print(f"Questions with answer set changes: {len(consolidated_data[consolidated_data['previous_answer_set'].notna()])}")
            print(f"Removed questions: {len(consolidated_data[consolidated_data['valid_to'].notna()])}")
        else:
            print("No data to summarize. The consolidated questions DataFrame is empty.")

        # Print detailed changes
        # print("\nDetailed Changes:")
        # for _, row in consolidated_data[consolidated_data['previous_answer_set'].notna() | consolidated_data['text_changed']].iterrows():
        #     print(f"\nQuestion ID: {row['question_id']}")
        #     print(f"Question Text: {row['question_text']}")
        #     if row['text_changed']:
        #         print("  Text changed from previous version")
        #     if row['previous_answer_set'] is not None:
        #         print("  Answer set changed")
        #         print(f"  Previous answer set: {row['previous_answer_set']}")
        #         print(f"  New answer set: {row['answer_set']}")

        # Print statistics
        # print("\nStatistics:")
        # print(f"Total questions: {len(consolidated_data)}")
        # print(f"Active questions: {len(consolidated_data[consolidated_data['is_active']])}")
        # print(f"Inactive questions: {len(consolidated_data[~consolidated_data['is_active']])}")
        # print(f"New questions: {len(consolidated_data[consolidated_data['first_seen_year'].notna()])}")
        # print(f"Questions with text changes: {len(consolidated_data[consolidated_data['text_changed']])}")
        # print(f"Questions with answer set changes: {len(consolidated_data[consolidated_data['previous_answer_set'].notna()])}")
        # print(f"Removed questions: {len(consolidated_data[consolidated_data['valid_to'].notna()])}")
        
        # print("\nSample of prepared data:")
        # prepared_data = consolidated_data.applymap(lambda x: None if pd.isna(x) or x == 'NaN' else x)
        # print(prepared_data.head().to_string())
        # print("\nPrepared data types:")
        # print(prepared_data.dtypes)
        
        print("\nSample of prepared data:")
        prepared_data = consolidated_data.copy()

        def prepare_value(value):
            if isinstance(value, (float, np.float64)) and np.isnan(value):
                return None
            if isinstance(value, list):
                return [prepare_value(v) for v in value if v is not None and not pd.isna(v)]
            if pd.isna(value):
                return None
            return value

        for column in prepared_data.columns:
            prepared_data[column] = prepared_data[column].apply(prepare_value)

        print(prepared_data.head().to_string())
        print("\nPrepared data types:")
        print(prepared_data.dtypes)
        print("\nNull values count:")
        print(prepared_data.isnull().sum())
        
        print("\nSample of data to be loaded into Snowflake:")
        print(consolidated_data.head().to_string())
        print("\nData types:")
        print(consolidated_data.dtypes)
        print("\nNull values count:")
        print(consolidated_data.isnull().sum())

        # Check for any remaining 'NaN' values
        for column in consolidated_data.columns:
            if column not in ['answer_set', 'previous_answer_set']:
                nan_count = consolidated_data[column].astype(str).str.contains('NaN').sum()
                if nan_count > 0:
                    print(f"Column {column} contains {nan_count} 'NaN' values")
        loader.load_data(consolidated_data)
        logger.info("Data loaded to Snowflake successfully")

        subprocess.run(['dbt', 'run', '--profiles-dir', 'dbt', '--project-dir', 'dbt'], check=True)
        logger.info("dbt run completed successfully")
    except subprocess.CalledProcessError as e:
        logger.error(f"dbt execution failed: {e}")
        logger.error(traceback.format_exc())
        sys.exit(1)

    except Exception as e:
        logger.error(f"An error occurred while finalizing the data: {str(e)}")
        logger.error(traceback.format_exc())

if __name__ == "__main__":
    main()
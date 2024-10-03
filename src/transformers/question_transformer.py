# # File: src/transformers/question_transformer.py

# import pandas as pd
# from fuzzywuzzy import fuzz
# from src.utils.logging_utils import setup_logger

# logger = setup_logger(__name__)

# class QuestionTransformer:
#     def __init__(self):
#         self.consolidated_questions = pd.DataFrame()

#     def _fuzzy_match_questions(self, question, existing_questions, threshold=80):
#         """Find a fuzzy match for a question in existing questions."""
#         for idx, existing_question in existing_questions.iterrows():
#             if fuzz.ratio(question, existing_question['question_text']) > threshold:
#                 return existing_question['question_id']
#         return None

#     def transform_questions(self, year_data, year):
#         """Transform questions data for a specific year."""
#         questions = year_data.drop_duplicates(subset=['QuestionID'])
        
#         for _, row in questions.iterrows():
#             question_text = row['QuestionText']
#             matched_id = self._fuzzy_match_questions(question_text, self.consolidated_questions)
            
#             if matched_id:
#                 # Update existing question
#                 self.consolidated_questions.loc[self.consolidated_questions['question_id'] == matched_id, 'valid_to'] = f"{year}-12-31"
#                 new_question = self.consolidated_questions.loc[self.consolidated_questions['question_id'] == matched_id].copy()
#                 new_question['valid_from'] = f"{year}-01-01"
#                 new_question['valid_to'] = None
#                 new_question['year'] = year
#                 new_question['question_text'] = question_text
#                 self.consolidated_questions = pd.concat([self.consolidated_questions, new_question])
#             else:
#                 # Add new question
#                 new_question = {
#                     'question_id': f"{year}_{row['QuestionID']}",
#                     'year': year,
#                     'question_text': question_text,
#                     'category': row['CategoryName'],
#                     'is_active': row['IsActive'],
#                     'valid_from': f"{year}-01-01",
#                     'valid_to': None
#                 }
#                 self.consolidated_questions = pd.concat([self.consolidated_questions, pd.DataFrame([new_question])])

#         # Transform answer options
#         answer_options = year_data.dropna(subset=['AnswerOptionID'])
#         answer_options_dict = answer_options.groupby('QuestionID').apply(
#             lambda x: x[['OptionText', 'OptionOrder']].to_dict('records')
#         ).to_dict()

#         self.consolidated_questions.loc[self.consolidated_questions['year'] == year, 'answer_set'] = \
#             self.consolidated_questions.loc[self.consolidated_questions['year'] == year, 'question_id'].map(
#                 lambda x: answer_options_dict.get(int(x.split('_')[1]), [])
#             )

#         logger.info(f"Transformed and consolidated questions for year {year}")

#     def get_consolidated_questions(self):
#         """Return the consolidated questions dataframe."""
#         return self.consolidated_questions

# # Example usage:
# if __name__ == "__main__":
#     from src.extractors.sql_server_extractor import SQLServerExtractor

#     extractor = SQLServerExtractor("config/sql_server_config.yaml")
#     transformer = QuestionTransformer()

#     for year in [2021, 2022, 2023]:
#         year_data = extractor.extract_questions_with_options(year)
#         if year_data is not None:
#             transformer.transform_questions(year_data, year)

#     consolidated_data = transformer.get_consolidated_questions()
#     print(consolidated_data.head())
#########################################################################################################
# File: src/transformers/question_transformer.py

# import pandas as pd
# from fuzzywuzzy import fuzz
# from src.utils.logging_utils import setup_logger

# logger = setup_logger(__name__)

# class QuestionTransformer:
#     def __init__(self):
#         self.consolidated_questions = pd.DataFrame()

#     def _fuzzy_match_questions(self, question, existing_questions, threshold=80):
#         """Find a fuzzy match for a question in existing questions."""
#         for idx, existing_question in existing_questions.iterrows():
#             if fuzz.ratio(question, existing_question['question_text']) > threshold:
#                 return existing_question['question_id']
#         return None

#     def transform_questions(self, year_data, year):
#         """Transform questions data for a specific year."""
#         # Validate if all required columns are present in year_data
#         required_columns = ['QuestionID', 'QuestionText', 'CategoryName', 'IsActive', 'AnswerOptionID', 'OptionText', 'OptionOrder']
#         missing_columns = [col for col in required_columns if col not in year_data.columns]
        
#         if missing_columns:
#             logger.error(f"Missing columns in extracted data for year {year}: {missing_columns}")
#             return

#         # Drop duplicates and extract relevant question data
#         questions = year_data.drop_duplicates(subset=['QuestionID'])

#         for _, row in questions.iterrows():
#             question_text = row['QuestionText']
#             matched_id = self._fuzzy_match_questions(question_text, self.consolidated_questions)
            
#             if matched_id:
#                 # Update existing question
#                 self.consolidated_questions.loc[self.consolidated_questions['question_id'] == matched_id, 'valid_to'] = f"{year}-12-31"
#                 new_question = self.consolidated_questions.loc[self.consolidated_questions['question_id'] == matched_id].copy()
#                 new_question['valid_from'] = f"{year}-01-01"
#                 new_question['valid_to'] = None
#                 new_question['year'] = year
#                 new_question['question_text'] = question_text
#                 self.consolidated_questions = pd.concat([self.consolidated_questions, new_question], ignore_index=True)
#             else:
#                 # Add new question
#                 new_question = {
#                     'question_id': f"{year}_{row['QuestionID']}",
#                     'year': year,
#                     'question_text': question_text,
#                     'category': row['CategoryName'],
#                     'is_active': row['IsActive'],
#                     'valid_from': f"{year}-01-01",
#                     'valid_to': None
#                 }
#                 self.consolidated_questions = pd.concat([self.consolidated_questions, pd.DataFrame([new_question])], ignore_index=True)

#         # Transform answer options if they exist
#         answer_options = year_data.dropna(subset=['AnswerOptionID'])
#         answer_options_dict = answer_options.groupby('QuestionID').apply(
#             lambda x: x[['OptionText', 'OptionOrder']].to_dict('records')
#         ).to_dict()

#         # Assign answer options to the corresponding questions
#         self.consolidated_questions.loc[self.consolidated_questions['year'] == year, 'answer_set'] = \
#             self.consolidated_questions.loc[self.consolidated_questions['year'] == year, 'question_id'].map(
#                 lambda x: answer_options_dict.get(int(x.split('_')[1]), [])
#             )

#         logger.info(f"Transformed and consolidated questions for year {year}")

#     def get_consolidated_questions(self):
#         """Return the consolidated questions dataframe."""
#         return self.consolidated_questions

# # Example usage:
# if __name__ == "__main__":
#     from src.extractors.sql_server_extractor import SQLServerExtractor

#     extractor = SQLServerExtractor("config/sql_server_config.yaml")
#     transformer = QuestionTransformer()

#     for year in [2021, 2022, 2023]:
#         year_data = extractor.extract_questions_with_options(year)
#         if year_data is not None:
#             transformer.transform_questions(year_data, year)

#     consolidated_data = transformer.get_consolidated_questions()
#     print(consolidated_data.head())
##########################################################################################################


# import pandas as pd
# import numpy as np
# from fuzzywuzzy import fuzz
# from src.utils.logging_utils import setup_logger
# import json

# logger = setup_logger(__name__)

# class QuestionTransformer:
#     def __init__(self):
#         self.consolidated_questions = pd.DataFrame()
#         self.yearly_questions = {}
#         self.all_question_ids = set()

#     def _fuzzy_match_questions(self, question, existing_questions, threshold=80):
#         """Find a fuzzy match for a question in existing questions."""
#         best_match = None
#         best_score = 0
#         for idx, existing_question in existing_questions.iterrows():
#             score = fuzz.ratio(question, existing_question['question_text'])
#             if score >= threshold and score > best_score:
#                 best_match = existing_question['question_id']
#                 best_score = score
#         return best_match, best_score

#     def _compare_answer_sets(self, old_set, new_set):
#         """Compare two answer sets and return if they are different."""
#         return json.dumps(old_set, sort_keys=True) != json.dumps(new_set, sort_keys=True)

#     def _prepare_value(self, value):
#         """Prepare value for storage, replacing NaN with None."""
#         if pd.isna(value) or (isinstance(value, float) and np.isnan(value)):
#             return None
#         return value

#     def transform_questions(self, year_data, year):
#         if year_data is None or year_data.empty:
#             logger.warning(f"No data to transform for year {year}")
#             return

#         # Replace NaN values with None in the input data for specific columns
#         for col in year_data.columns:
#             if col not in ['answer_set', 'previous_answer_set']:
#                 year_data[col] = year_data[col].apply(self._prepare_value)

#         self.yearly_questions[year] = year_data
#         new_questions = []
#         processed_questions = set()

#         for _, row in year_data.iterrows():
#             question_text = row['question_text']
#             current_question_id = f"{year}_{row['question_id']}"
            
#             matched_id = None
#             match_score = 0
#             previous_version = None
#             answer_set_changed = False

#             for prev_year in range(year - 1, year - 3, -1):
#                 if prev_year in self.yearly_questions:
#                     matched_id, match_score = self._fuzzy_match_questions(
#                         question_text, 
#                         self.yearly_questions[prev_year], 
#                         threshold=70
#                     )
#                     if matched_id:
#                         previous_versions = self.consolidated_questions[self.consolidated_questions['question_id'] == matched_id]
#                         if not previous_versions.empty:
#                             previous_version = previous_versions.iloc[-1]
#                             answer_set_changed = self._compare_answer_sets(previous_version['answer_set'], row['answer_set'])
#                         break

#             if matched_id and previous_version is not None:
#                 text_changed = question_text != previous_version['question_text']
                
#                 if text_changed or answer_set_changed:
#                     # Question text or answer set changed, create a new version
#                     new_question_id = current_question_id
#                     # Update valid_to for the previous version
#                     self.consolidated_questions.loc[self.consolidated_questions['question_id'] == matched_id, 'valid_to'] = year - 1
#                     answer_set_version = previous_version['answer_set_version'] + 1
#                     previous_answer_set = previous_version['answer_set'] if answer_set_changed else None
#                 else:
#                     new_question_id = matched_id
#                     answer_set_version = previous_version['answer_set_version']
#                     previous_answer_set = None
#             else:
#                 # New question
#                 new_question_id = current_question_id
#                 text_changed = False
#                 answer_set_version = 1
#                 previous_answer_set = None

#             new_question = {
#                 'question_id': new_question_id,
#                 'year': year,
#                 'question_text': question_text,
#                 'category': row['category'],
#                 'is_active': True,
#                 'answer_set': row['answer_set'],
#                 'previous_answer_set': previous_answer_set,
#                 'valid_to': None,
#                 'first_seen_year': year if not matched_id else None,
#                 'previous_version_id': matched_id if matched_id else None,
#                 'match_score': match_score if matched_id else None,
#                 'answer_set_version': answer_set_version,
#                 'text_changed': text_changed
#             }

#             # Ensure all values are not NaN
#             for key, value in new_question.items():
#                 if key not in ['answer_set', 'previous_answer_set']:
#                     new_question[key] = self._prepare_value(value)

#             new_questions.append(new_question)
#             processed_questions.add(new_question_id)
#             self.all_question_ids.add(new_question_id)

#         # Handle removed questions
#         for prev_year in range(year - 1, year - 3, -1):
#             if prev_year in self.yearly_questions:
#                 for _, prev_question in self.yearly_questions[prev_year].iterrows():
#                     prev_id = f"{prev_year}_{prev_question['question_id']}"
#                     if prev_id in self.all_question_ids and prev_id not in processed_questions:
#                         # This question was in a previous year but not in the current year
#                         self.consolidated_questions.loc[self.consolidated_questions['question_id'] == prev_id, 'valid_to'] = year - 1
#                         self.consolidated_questions.loc[self.consolidated_questions['question_id'] == prev_id, 'is_active'] = False

#         if new_questions:
#             new_df = pd.DataFrame(new_questions)
#             self.consolidated_questions = pd.concat([self.consolidated_questions, new_df], ignore_index=True)

#         # Set is_active to False for questions that are not in the current year
#         self.consolidated_questions.loc[
#             (self.consolidated_questions['year'] != year) & 
#             (self.consolidated_questions['valid_to'].isnull()), 'is_active'
#         ] = False

#         logger.info(f"Transformed and consolidated questions for year {year}")

#     def get_consolidated_questions(self):
#         """Return the consolidated questions dataframe."""
#         if self.consolidated_questions.empty:
#             return pd.DataFrame(columns=['question_id', 'year', 'question_text', 'category', 'is_active', 
#                                          'answer_set', 'previous_answer_set', 'valid_to', 'first_seen_year', 
#                                          'previous_version_id', 'match_score', 'answer_set_version', 'text_changed'])
#         return self.consolidated_questions

# import pandas as pd
# from fuzzywuzzy import fuzz
# from src.utils.logging_utils import setup_logger

# logger = setup_logger(__name__)

# class QuestionTransformer:
#     def __init__(self):
#         self.consolidated_questions = pd.DataFrame()
#         self.schema_changes = {}

#     def _fuzzy_match_questions(self, question, existing_questions, threshold=80):
#         """Find a fuzzy match for a question in existing questions."""
#         for idx, existing_question in existing_questions.iterrows():
#             if fuzz.ratio(question, existing_question['question_text']) > threshold:
#                 return existing_question['question_id']
#         return None
    
#     def _detect_schema_changes(self, year_data, year):
#         current_schema = set(year_data.columns)
#         if year - 1 in self.schema_changes:
#             previous_schema = set(self.schema_changes[year - 1])
#             new_columns = current_schema - previous_schema
#             removed_columns = previous_schema - current_schema
#             if new_columns or removed_columns:
#                 logger.info(f"Schema changes detected for year {year}")
#                 logger.info(f"New columns: {new_columns}")
#                 logger.info(f"Removed columns: {removed_columns}")
#         self.schema_changes[year] = current_schema


#     def transform_questions(self, year_data, year):
#         """Transform questions data for a specific year."""
#         if year_data is None or year_data.empty:
#             logger.warning(f"No data to transform for year {year}")
#             return

#         # Validate if all required columns are present in year_data
#         required_columns = ['question_id', 'question_text', 'category', 'is_active', 'question_order', 'answer_set']
#         missing_columns = [col for col in required_columns if col not in year_data.columns]
        
#         if missing_columns:
#             logger.error(f"Missing columns in extracted data for year {year}: {missing_columns}")
#             return

#         new_questions = []
#         for _, row in year_data.iterrows():
#             question_text = row['question_text']
#             matched_id = self._fuzzy_match_questions(question_text, self.consolidated_questions)
            
#             if matched_id:
#                 # Update existing question
#                 self.consolidated_questions.loc[self.consolidated_questions['question_id'] == matched_id, 'valid_to'] = f"{year}-12-31"
#                 new_question = {
#                     'question_id': matched_id,
#                     'year': year,
#                     'question_text': question_text,
#                     'category': row['category'],
#                     'is_active': row['is_active'],
#                     'question_order': row['question_order'],
#                     'answer_set': row['answer_set'],
#                     'valid_from': f"{year}-01-01",
#                     'valid_to': None
#                 }
#             else:
#                 # Add new question
#                 new_question = {
#                     'question_id': row['question_id'],
#                     'year': year,
#                     'question_text': question_text,
#                     'category': row['category'],
#                     'is_active': row['is_active'],
#                     'question_order': row['question_order'],
#                     'answer_set': row['answer_set'],
#                     'valid_from': f"{year}-01-01",
#                     'valid_to': None
#                 }
#             new_questions.append(new_question)

#         # Concatenate all new questions
#         if new_questions:
#             new_questions_df = pd.DataFrame(new_questions)
#             self.consolidated_questions = pd.concat([self.consolidated_questions, new_questions_df], ignore_index=True)

#         logger.info(f"Transformed and consolidated questions for year {year}")

   
        
#     def get_consolidated_questions(self):
#         """Return the consolidated questions dataframe."""
#         return self.consolidated_questions

# # Example usage:
# if __name__ == "__main__":
#     from src.extractors.sql_server_extractor import SQLServerExtractor

#     extractor = SQLServerExtractor("config/sql_server_config.yaml")
#     transformer = QuestionTransformer()

#     for year in [2021, 2022, 2023]:
#         year_data = extractor.extract_questions_with_options(year)
#         if year_data is not None:
#             transformer.transform_questions(year_data, year)

#     consolidated_data = transformer.get_consolidated_questions()
#     print(consolidated_data.head())
#     print(f"Total questions: {len(consolidated_data)}")
#     print(f"Columns: {consolidated_data.columns}")
import pandas as pd
import numpy as np
from typing import Dict, List, Any, Tuple
import logging
from fuzzywuzzy import fuzz
from fuzzywuzzy import process

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ResearchQuestionTransformer:
    def __init__(self, data: pd.DataFrame, metadata: Dict[int, Dict[str, Any]]):
        self.data = data
        self.metadata = metadata
        self.unified_data = None
        self.question_mapping = {}
        self.answer_mapping = {}
        self.fuzzy_threshold = 70
        self.required_columns = ['QuestionText', 'OptionText', 'Year', 'CategoryName']

    def _validate_data(self):
        """Validate the input data structure."""
        missing_columns = [col for col in self.required_columns if col not in self.data.columns]
        if missing_columns:
            raise ValueError(f"Data is missing required columns: {', '.join(missing_columns)}")

    def fuzzy_match(self, text: str, choices: List[str], threshold: int) -> Tuple[str, int]:
        """Perform fuzzy matching and return the best match above the threshold."""
        best_match, score = process.extractOne(text, choices, scorer=fuzz.token_sort_ratio)
        if score >= threshold:
            return best_match, score
        return None, score

    def create_question_mapping(self):
        """Create a mapping of questions using fuzzy matching."""
        all_questions = self.data[['QuestionText', 'Year', 'CategoryName']].drop_duplicates()
        
        for _, row in all_questions.iterrows():
            question = row['QuestionText']
            year = row['Year']
            category = row['CategoryName']
            
            if not self.question_mapping:
                self.question_mapping[question] = {'id': 1, 'years': [year], 'category': category}
            else:
                match, score = self.fuzzy_match(question, list(self.question_mapping.keys()), self.fuzzy_threshold)
                if match:
                    logger.info(f"Fuzzy matched question: '{question}' to '{match}' (score: {score})")
                    self.question_mapping[match]['years'].append(year)
                    if category != self.question_mapping[match]['category']:
                        logger.warning(f"Category change for question '{match}': {self.question_mapping[match]['category']} -> {category}")
                else:
                    new_id = max(item['id'] for item in self.question_mapping.values()) + 1
                    self.question_mapping[question] = {'id': new_id, 'years': [year], 'category': category}

    def create_answer_mapping(self):
        """Create a mapping of answer options using fuzzy matching."""
        all_answers = self.data[['OptionText', 'Year', 'QuestionText']].drop_duplicates()
        
        for _, row in all_answers.iterrows():
            answer = row['OptionText']
            year = row['Year']
            question = row['QuestionText']
            
            if not self.answer_mapping:
                self.answer_mapping[answer] = {'id': 1, 'years': [year], 'questions': [question]}
            else:
                match, score = self.fuzzy_match(answer, list(self.answer_mapping.keys()), self.fuzzy_threshold)
                if match:
                    logger.info(f"Fuzzy matched answer: '{answer}' to '{match}' (score: {score})")
                    self.answer_mapping[match]['years'].append(year)
                    if question not in self.answer_mapping[match]['questions']:
                        self.answer_mapping[match]['questions'].append(question)
                else:
                    new_id = max(item['id'] for item in self.answer_mapping.values()) + 1
                    self.answer_mapping[answer] = {'id': new_id, 'years': [year], 'questions': [question]}

    def transform_data(self):
        """Transform the data into a unified structure using fuzzy matching."""
        self.create_question_mapping()
        self.create_answer_mapping()

        self.unified_data = self.data.copy()
        self.unified_data['QuestionID'] = self.unified_data['QuestionText'].map(lambda x: next(item['id'] for item in self.question_mapping.values() if x in item['years']))
        self.unified_data['AnswerID'] = self.unified_data['OptionText'].map(lambda x: next(item['id'] for item in self.answer_mapping.values() if x in item['years']))

    def analyze_changes(self) -> Dict[str, Any]:
        """Analyze changes in questions and answers over years."""
        changes = {
            'new_questions': {},
            'modified_questions': {},
            'new_answers': {},
            'modified_answers': {},
            'category_changes': {}
        }

        years = sorted(self.unified_data['Year'].unique())
        
        for i in range(1, len(years)):
            prev_year, curr_year = years[i-1], years[i]
            
            # Analyze question changes
            for question, info in self.question_mapping.items():
                if curr_year in info['years'] and prev_year not in info['years']:
                    changes['new_questions'].setdefault(curr_year, []).append(question)
                elif curr_year in info['years'] and prev_year in info['years']:
                    prev_text = self.unified_data[(self.unified_data['Year'] == prev_year) & (self.unified_data['QuestionID'] == info['id'])]['QuestionText'].iloc[0]
                    curr_text = self.unified_data[(self.unified_data['Year'] == curr_year) & (self.unified_data['QuestionID'] == info['id'])]['QuestionText'].iloc[0]
                    if prev_text != curr_text:
                        changes['modified_questions'].setdefault(curr_year, []).append((prev_text, curr_text))

            # Analyze answer changes
            for answer, info in self.answer_mapping.items():
                if curr_year in info['years'] and prev_year not in info['years']:
                    changes['new_answers'].setdefault(curr_year, []).append(answer)
                elif curr_year in info['years'] and prev_year in info['years']:
                    prev_text = self.unified_data[(self.unified_data['Year'] == prev_year) & (self.unified_data['AnswerID'] == info['id'])]['OptionText'].iloc[0]
                    curr_text = self.unified_data[(self.unified_data['Year'] == curr_year) & (self.unified_data['AnswerID'] == info['id'])]['OptionText'].iloc[0]
                    if prev_text != curr_text:
                        changes['modified_answers'].setdefault(curr_year, []).append((prev_text, curr_text))

            # Analyze category changes
            for question, info in self.question_mapping.items():
                if curr_year in info['years'] and prev_year in info['years']:
                    prev_category = self.unified_data[(self.unified_data['Year'] == prev_year) & (self.unified_data['QuestionID'] == info['id'])]['CategoryName'].iloc[0]
                    curr_category = self.unified_data[(self.unified_data['Year'] == curr_year) & (self.unified_data['QuestionID'] == info['id'])]['CategoryName'].iloc[0]
                    if prev_category != curr_category:
                        changes['category_changes'].setdefault(curr_year, []).append((question, prev_category, curr_category))

        return changes

    def run_transformation(self) -> Dict[str, Any]:
        """Run the full transformation process with fuzzy matching."""
        self._validate_data()
        self.transform_data()
        changes = self.analyze_changes()

        return {
            'unified_data': self.unified_data,
            'question_mapping': self.question_mapping,
            'answer_mapping': self.answer_mapping,
            'changes_analysis': changes
        }

# Example usage
if __name__ == "__main__":
    # Assuming you have data and metadata from SQLServerExtractor
    extractor = SQLServerExtractor("sql_server_config.yaml")
    extraction_result = extractor.extract_all_data()

    transformer = ResearchQuestionTransformer(extraction_result['combined_data'], extraction_result['metadata'])
    transformation_result = transformer.run_transformation()

    # Save results
    transformation_result['unified_data'].to_csv("unified_research_questions.csv", index=False)
    transformation_result['question_evolution'].to_csv("question_evolution.csv", index=False)
    transformation_result['answer_evolution'].to_csv("answer_evolution.csv", index=False)

    print("Transformation complete. Results saved to CSV files.")
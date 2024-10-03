import pandas as pd

def check_data_quality(df: pd.DataFrame) -> list:
    issues = []
    
    # Check for null values
    null_counts = df.isnull().sum()
    if null_counts.any():
        issues.append(f"Null values found: {null_counts[null_counts > 0].to_dict()}")
    
    # Check for duplicate question IDs within the same year
    duplicates = df[df.duplicated(subset=['question_id', 'year'], keep=False)]
    if not duplicates.empty:
        issues.append(f"Duplicate question IDs found: {duplicates['question_id'].tolist()}")
    
    # Check for invalid years
    invalid_years = df[~df['year'].isin([2021, 2022, 2023])]
    if not invalid_years.empty:
        issues.append(f"Invalid years found: {invalid_years['year'].unique().tolist()}")
    
    # Check for empty answer sets
    empty_answer_sets = df[df['answer_set'].apply(lambda x: len(eval(x)) == 0)]
    if not empty_answer_sets.empty:
        issues.append(f"Empty answer sets found for questions: {empty_answer_sets['question_id'].tolist()}")
    
    return issues
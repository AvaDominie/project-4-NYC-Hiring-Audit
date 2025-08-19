import pandas as pd

def process_payroll_data(csv_path, nrows=1000):
    # Read a subset for development
    df = pd.read_csv(csv_path, nrows=nrows)
    
    # Select relevant columns (adjust names as needed)
    cols = ['AGENCY_NAME', 'TITLE_DESCRIPTION', 'BASE_SALARY', 'PAY_BASIS']
    df = df[cols]
    
    # Standardize salary to annual
    df['ANNUAL_SALARY'] = df.apply(
        lambda row: row['BASE_SALARY'] * 2080 if str(row['PAY_BASIS']).upper() == 'HOURLY' else row['BASE_SALARY'],
        axis=1
    )
    
    # Create a fuzzy matching string
    df['MATCH_STRING'] = df['AGENCY_NAME'].str.lower().str.strip() + ' ' + df['TITLE_DESCRIPTION'].str.lower().str.strip()
    
    return df[['AGENCY_NAME', 'TITLE_DESCRIPTION', 'ANNUAL_SALARY', 'MATCH_STRING']]
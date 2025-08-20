import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
# Utility function to upload a DataFrame to Snowflake GOLD schema
def upload_to_snowflake_gold(df, table_name, gold_schema=None):
    """
    Uploads a DataFrame to the GOLD schema in Snowflake using write_pandas.
    Requires SNOWFLAKE connection environment variables to be set.
    """
    SNOWFLAKE_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
    SNOWFLAKE_USER = os.getenv('SNOWFLAKE_USER')
    SNOWFLAKE_PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
    SNOWFLAKE_DATABASE = os.getenv('SNOWFLAKE_DATABASE')
    SNOWFLAKE_WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE')
    SNOWFLAKE_ROLE = os.getenv('SNOWFLAKE_ROLE')
    SNOWFLAKE_SCHEMA_GOLD = gold_schema or os.getenv('SNOWFLAKE_SCHEMA_GOLD', 'GOLD')

    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA_GOLD,
        warehouse=SNOWFLAKE_WAREHOUSE,
        role=SNOWFLAKE_ROLE
    )
    cursor = conn.cursor()
    try:
        cursor.execute(f"USE DATABASE {SNOWFLAKE_DATABASE}")
        cursor.execute(f"USE SCHEMA {SNOWFLAKE_SCHEMA_GOLD}")
        df["LOAD_TIMESTAMP_UTC"] = pd.Timestamp.utcnow()
        df.columns = df.columns.str.upper()
        success, nchunks, nrows, _ = write_pandas(
            conn=conn,
            df=df,
            table_name=table_name.upper(),
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA_GOLD,
            auto_create_table=True,
            overwrite=True,
            quote_identifiers=True,
            use_logical_type=True
        )
        if success:
            print(f"Data uploaded to Snowflake GOLD.{table_name.upper()} ({nrows} rows in {nchunks} chunks)")
        else:
            print(f"Failed to upload data to Snowflake GOLD.{table_name.upper()}")
    finally:
        cursor.close()
        conn.close()
import io
import logging
from logging.handlers import RotatingFileHandler
import pandas as pd
import requests
import os
from dotenv import load_dotenv




load_dotenv()

# set up logging
VSCODE_WORKSPACE_FOLDER = os.getenv('VSCODE_WORKSPACE_FOLDER', os.getcwd())
LOG_DIR = os.path.join(VSCODE_WORKSPACE_FOLDER, 'src/logs')
os.makedirs(LOG_DIR, exist_ok=True)

LOG_FILE = os.path.join(LOG_DIR, 'fuzzy_match.log')


logging.getLogger("fuzzy_match").handlers.clear()

logger = logging.getLogger("fuzzy_match")
logger.setLevel(logging.INFO)

file_handler = RotatingFileHandler(LOG_FILE, maxBytes=10*1024*1024, backupCount=5)
file_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)



# Cleans and standardizes payroll data for fuzzy matching
def process_payroll_data(data, nrows=1000):
    """
    Accepts either a CSV file path or a DataFrame and processes payroll data for fuzzy matching.
    """

    logger.info("Starting process_payroll_data with nrows=%d", nrows)
    # If data is a string, treat as CSV path; if DataFrame, use directly
    if isinstance(data, str):
        logger.info("Reading payroll data from CSV: %s", data)
        df = pd.read_csv(data, nrows=nrows)
    else:
        logger.info("Processing payroll data from DataFrame input.")
        df = data.copy()


    # Select relevant columns (adjust names as needed)
    cols = ['AGENCY_NAME', 'TITLE_DESCRIPTION', 'BASE_SALARY', 'PAY_BASIS']
    # Handle case-insensitive column names from API
    df.columns = [col.upper() for col in df.columns]
    logger.info("Payroll DataFrame columns after upper: %s", df.columns.tolist())
    df = df[[col for col in cols if col in df.columns]]

    # Standardize salary to annual
    if 'BASE_SALARY' in df.columns and 'PAY_BASIS' in df.columns:
        df['BASE_SALARY'] = pd.to_numeric(df['BASE_SALARY'], errors='coerce')
        df['ANNUAL_SALARY'] = df.apply(
            lambda row: row['BASE_SALARY'] * 2080 if str(row['PAY_BASIS']).upper() == 'PER HOUR' else row['BASE_SALARY'],
            axis=1
        )
    else:
        df['ANNUAL_SALARY'] = None

    # Create a fuzzy matching string
    if 'AGENCY_NAME' in df.columns and 'TITLE_DESCRIPTION' in df.columns:
        df['MATCH_STRING'] = df['AGENCY_NAME'].str.lower().str.strip() + ' ' + df['TITLE_DESCRIPTION'].str.lower().str.strip()
    else:
        df['MATCH_STRING'] = ''

    logger.info("Finished process_payroll_data. Returning processed DataFrame with shape %s", df.shape)
    return df[['AGENCY_NAME', 'TITLE_DESCRIPTION', 'ANNUAL_SALARY', 'MATCH_STRING']]



# Finds the best fuzzy match for a string from a list of candidates
def calculate_fuzzy_match(target_string, candidate_list_of_strings, use_rapidfuzz=True):

    logger.info("Starting calculate_fuzzy_match for target: '%s' with %d candidates (use_rapidfuzz=%s)", target_string, len(candidate_list_of_strings), use_rapidfuzz)
    """
    Find the best fuzzy match for a target string from a list of candidate strings.
    Uses either rapidfuzz or fuzzywuzzy's token_set_ratio for comparison.

    Args:
        target_string (str): The string to match against the candidates.
        candidate_list_of_strings (list of str): List of possible match strings.
        use_rapidfuzz (bool): If True, use rapidfuzz (faster); else use fuzzywuzzy.

    Returns:
        tuple: (best_match_string, best_ratio) where best_match_string is the candidate with the highest score,
               and best_ratio is the similarity score (0-100).
    """

    # Choose the fuzzy matching library based on the flag
    if use_rapidfuzz:
        # rapidfuzz is faster and has a similar API to fuzzywuzzy
        from rapidfuzz import fuzz
    else:
        # fuzzywuzzy is slower but widely used and has the same API
        from fuzzywuzzy import fuzz

    best_match = None  # Store the best matching string found so far
    best_ratio = -1    # Store the highest similarity ratio found so far

    # Iterate through each candidate string
    for candidate in candidate_list_of_strings:
        # Compute the token_set_ratio between the target and candidate
        # token_set_ratio is robust to word order and partial overlaps, good for job titles
        ratio = fuzz.token_set_ratio(target_string, candidate)
        # If this candidate is a better match, update best_match and best_ratio
        if ratio > best_ratio:
            best_ratio = ratio
            best_match = candidate

    # Return the best match and its similarity ratio
    logger.info("Best match for '%s' is '%s' with ratio %.2f", target_string, best_match, best_ratio)
    return best_match, best_ratio



# Processes job postings, calculates posting duration, and applies fuzzy matching to enrich with payroll data
def process_job_postings_data(payroll_lookup_df, job_postings_path, nrows=1000, match_threshold=85):
    logger.info("Starting process_job_postings_data with nrows=%d, match_threshold=%d", nrows, match_threshold)
    logger.info("Reading job postings from: %s", job_postings_path)
    """
    Process job postings, calculate duration, and apply fuzzy matching for the first deliverable.
    Args:
        payroll_lookup_df (pd.DataFrame): DataFrame with payroll lookup and MATCH_STRING column.
        job_postings_path (str): Path to the job postings CSV file.
        nrows (int): Number of rows to read for development.
        match_threshold (int): Minimum match ratio to include payroll salary info.
    Returns:
        pd.DataFrame: Processed job postings with fuzzy match info and salary if match is strong.
    """

    # 1. Read job postings data
    jobs_df = pd.read_csv(job_postings_path, nrows=nrows)
    logger.info("Job postings columns: %s", jobs_df.columns.tolist())
    logger.info("Rows after reading: %d", len(jobs_df))
    logger.debug("First 5 rows after reading:\n%s", jobs_df.head())

    # Normalize column names to lowercase for consistency
    jobs_df.columns = [col.lower() for col in jobs_df.columns]
    logger.info("Job postings columns after lower: %s", jobs_df.columns.tolist())
    logger.debug("First 5 rows after column normalization:\n%s", jobs_df.head())

    # 2. Parse posting_date and filter for 2024 or 2025
    try:
        jobs_df['posting_date'] = pd.to_datetime(jobs_df['posting_date'], errors='coerce')
        logger.info("Sample posting_date values: %s", jobs_df['posting_date'].head(10).tolist())
        logger.info("posting_date year value counts: %s", jobs_df['posting_date'].dt.year.value_counts().to_dict())
        jobs_df = jobs_df[jobs_df['posting_date'].dt.year.isin([2024, 2025])]
        logger.info("Rows after date filter: %d", len(jobs_df))
        logger.debug("First 5 rows after date filter:\n%s", jobs_df.head())
    except Exception as e:
        logger.error("Error processing 'posting_date': %s", e)
        raise

    # 3. Calculate Posting Duration
    try:
        jobs_df['post_until'] = pd.to_datetime(jobs_df['post_until'], errors='coerce')
        today = pd.Timestamp.today()
        jobs_df['posting_duration'] = (jobs_df['post_until'].fillna(today) - jobs_df['posting_date']).dt.days
        jobs_df['posting_duration'] = jobs_df['posting_duration'].fillna(30).astype(int)
        logger.debug("First 5 rows after posting_duration calculation:\n%s", jobs_df.head())
    except Exception as e:
        logger.error("Error processing 'post_until' or 'posting_duration': %s", e)
        raise

    # 4. Fuzzy match and enrich
    match_strings = payroll_lookup_df['MATCH_STRING'].tolist()
    min_salaries = []
    max_salaries = []
    match_ratios = []
    best_matches = []
    # Use agency + business_title for better matching
    for idx, row in jobs_df.iterrows():
        agency = str(row.get('agency', '')).lower().strip()
        business_title = str(row.get('business_title', '')).lower().strip()
        job_match_string = f"{agency} {business_title}".strip()
        best_match, ratio = calculate_fuzzy_match(job_match_string, match_strings)
        logger.info("Job posting '%s' (agency: '%s') best match: '%s' (ratio: %.2f)", business_title, agency, best_match, ratio)
        if ratio >= match_threshold:
            idx_payroll = payroll_lookup_df['MATCH_STRING'] == best_match
            min_salary = payroll_lookup_df.loc[idx_payroll, 'ANNUAL_SALARY'].min()
            max_salary = payroll_lookup_df.loc[idx_payroll, 'ANNUAL_SALARY'].max()
        else:
            min_salary = None
            max_salary = None
        min_salaries.append(min_salary)
        max_salaries.append(max_salary)
        match_ratios.append(ratio)
        best_matches.append(best_match)
    jobs_df['Payroll Salary Min'] = min_salaries
    jobs_df['Payroll Salary Max'] = max_salaries
    jobs_df['Match Ratio'] = match_ratios
    jobs_df['Best Payroll Match'] = best_matches
    logger.debug("First 5 rows after fuzzy matching:\n%s", jobs_df.head())

    # 5. Filter for strong matches
    logger.info("Rows before match ratio filter: %d", len(jobs_df))
    jobs_df = jobs_df[jobs_df['Match Ratio'] >= match_threshold]
    logger.info("Rows after match ratio filter: %d", len(jobs_df))
    logger.debug("First 5 rows after match ratio filter:\n%s", jobs_df.head())

    # 6. Select final columns
    # Ensure all output columns are lowercase for consistency
    jobs_df = jobs_df.rename(columns={
        'Payroll Salary Min': 'payroll_salary_min',
        'Payroll Salary Max': 'payroll_salary_max',
        'Match Ratio': 'match_ratio',
        'Best Payroll Match': 'best_payroll_match'
    })
    final_cols = [
        'business_title', 'posting_date', 'post_until', 'posting_duration',
        'payroll_salary_min', 'payroll_salary_max', 'match_ratio', 'best_payroll_match'
    ]
    logger.info("Finished process_job_postings_data. Returning DataFrame with shape %s", jobs_df[final_cols].shape)
    logger.debug("First 5 rows of final output:\n%s", jobs_df[final_cols].head())
    return jobs_df[final_cols]






# --- Lighthouse Data Processing and Posting Duration Dataset ---
def process_lighthouse_data(lighthouse_path):
    """
    Reads and cleans the Lighthouse Excel data.
    Args:
        lighthouse_path (str): Path to the Lighthouse Excel file.
    Returns:
        pd.DataFrame: Cleaned Lighthouse DataFrame.
    """
    # Read the correct sheet and skip the first two rows to get the actual header
    df = pd.read_excel(lighthouse_path, sheet_name='Job Postings Job Titles', header=2)
    # Rename columns to standard names
    rename_map = {
        'Job Title': 'lighthouse_job_title',
        'Total Postings (Jan 2024 - Jun 2025)': 'lighthouse_total_postings',
        'Median Posting Duration': 'lighthouse_median_posting_duration'
    }
    df = df.rename(columns=rename_map)
    # Remove any rows where job title is NaN or not a string
    df = df[df['lighthouse_job_title'].notnull()]
    # Clean up posting duration to be integer days if needed
    if 'lighthouse_median_posting_duration' in df.columns:
        df['lighthouse_median_posting_duration'] = df['lighthouse_median_posting_duration'].astype(str).str.extract(r'(\d+)').astype(float)
    # Select only the columns we need
    required_cols = ['lighthouse_job_title', 'lighthouse_total_postings', 'lighthouse_median_posting_duration']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing expected columns in Lighthouse data: {missing_cols}")
    return df[required_cols].reset_index(drop=True)

def calculate_fuzzy_match_lighthouse(target_string, candidate_list_of_strings):
    from rapidfuzz import fuzz
    best_match = None
    best_ratio = -1
    for candidate in candidate_list_of_strings:
        ratio = fuzz.token_set_ratio(target_string, candidate)
        if ratio > best_ratio:
            best_ratio = ratio
            best_match = candidate
    return best_match, best_ratio

def generate_posting_duration_dataset(processed_jobs_df, processed_lighthouse_df, match_threshold=75):
    """
    Fuzzy match job postings to Lighthouse data and combine for posting duration analysis.
    Args:
        processed_jobs_df (pd.DataFrame): Cleaned job postings DataFrame.
        processed_lighthouse_df (pd.DataFrame): Cleaned Lighthouse DataFrame.
        match_threshold (int): Minimum match ratio for inclusion.
    Returns:
        pd.DataFrame: Combined dataset for posting duration analysis.
    """
    lighthouse_titles = processed_lighthouse_df['lighthouse_job_title'].astype(str).tolist()
    matched_titles = []
    match_ratios = []
    total_postings = []
    median_durations = []
    for title in processed_jobs_df['business_title']:
        best_match, ratio = calculate_fuzzy_match_lighthouse(str(title), lighthouse_titles)
        if ratio >= match_threshold:
            idx = processed_lighthouse_df['lighthouse_job_title'] == best_match
            total_post = processed_lighthouse_df.loc[idx, 'lighthouse_total_postings'].values[0]
            median_duration = processed_lighthouse_df.loc[idx, 'lighthouse_median_posting_duration'].values[0]
        else:
            best_match = None
            total_post = None
            median_duration = None
        matched_titles.append(best_match)
        match_ratios.append(ratio)
        total_postings.append(total_post)
        median_durations.append(median_duration)
    result = processed_jobs_df.copy()
    result['lighthouse_job_title'] = matched_titles
    result['lighthouse_match_ratio'] = match_ratios
    result['lighthouse_total_postings'] = total_postings
    result['lighthouse_median_posting_duration'] = median_durations
    # Select final columns
    final_cols = [
        'business_title', 'posting_date', 'post_until', 'posting_duration',
        'payroll_salary_min', 'payroll_salary_max', 'match_ratio', 'best_payroll_match',
        'lighthouse_job_title', 'lighthouse_match_ratio', 'lighthouse_total_postings', 'lighthouse_median_posting_duration'
    ]
    return result[final_cols]





def main():
    # Download payroll data from the API endpoint
    PAY_ROLL = os.getenv('PAY_ROLL_ENDPOINT')
    response = requests.get(PAY_ROLL)
    data = response.json()

    # Convert to DataFrame
    df = pd.DataFrame(data)

    # Process the payroll data (you may want to adjust nrows or columns as needed)
    df = process_payroll_data(df)


    # Fuzzy match the first five job postings' business_title against payroll
    JOB_POSTINGS_RESULT = os.getenv('JOB_POSTING_ENDPOINT')
    response = requests.get(JOB_POSTINGS_RESULT)
    try:
        job_postings_file = io.StringIO(response.content.decode('utf-8'))
        job_postings_df = pd.read_csv(job_postings_file)
        job_postings_df.columns = [col.lower() for col in job_postings_df.columns]
        print("\nFuzzy match results for first five job postings:")
        for i, row in job_postings_df.head(5).iterrows():
            business_title = str(row['business_title'])
            best_match, score = calculate_fuzzy_match(business_title, df['MATCH_STRING'].tolist())
            print(f"Job Posting: {business_title}\n  Best Payroll Match: {best_match}\n  Score: {score}\n")



        # Now process all job postings for the deliverable
        job_postings_file.seek(0)  # Reset file pointer
        job_postings_result = process_job_postings_data(df, job_postings_file)
        print("\nProcessed Job Postings (first 5 rows):")
        print(job_postings_result.head())
    except Exception as e:
        print(f"Error processing job postings: {e}")


    # --- Lighthouse Data Processing and Posting Duration Dataset ---
    # Get Lighthouse file path from environment variable or config
    LIGHTHOUSE_FILE = os.getenv('LIGHTHOUSE_XLSX_PATH', 'data/Job Posting Analytics New York NY.xls')
    try:
        processed_lighthouse_df = process_lighthouse_data(LIGHTHOUSE_FILE)
        print("\nProcessed Lighthouse Data (first 5 rows):")
        print(processed_lighthouse_df.head())

        # Generate posting duration dataset by fuzzy matching job postings to Lighthouse
        posting_duration_df = generate_posting_duration_dataset(job_postings_result, processed_lighthouse_df, match_threshold=75)
        print("\nCombined Posting Duration Dataset (first 5 rows):")
        print(posting_duration_df.head())

        # --- Upload to Snowflake GOLD schema ---
        print("\nUploading processed datasets to Snowflake GOLD schema...")
        upload_to_snowflake_gold(job_postings_result, table_name="job_postings_gold")
        upload_to_snowflake_gold(processed_lighthouse_df, table_name="lighthouse_gold")
        upload_to_snowflake_gold(posting_duration_df, table_name="posting_duration_gold")
        print("Upload to GOLD schema complete.")
    except Exception as e:
        print(f"Error processing Lighthouse data or generating posting duration dataset: {e}")






if __name__ == "__main__":
    main()

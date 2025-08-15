import datetime
from io import BytesIO
import json
import os
import logging
from time import timezone
import pandas as pd
import io
import requests
import minio
from snowflake.connector.pandas_tools import write_pandas
from minio.error import S3Error
import snowflake.connector
from dotenv import load_dotenv
from minio import Minio
import polars as pl





load_dotenv()




# Logging setup
VSCODE_WORKSPACE_FOLDER = os.getenv('VSCODE_WORKSPACE_FOLDER', os.getcwd())
LOG_DIR = os.path.join(VSCODE_WORKSPACE_FOLDER, 'src/logs')
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, 'data_staging.log')

logging.basicConfig(
	level=logging.INFO,
	format='%(asctime)s - %(levelname)s - %(message)s',
	handlers=[
		logging.FileHandler(LOG_FILE),
		logging.StreamHandler()
	]
)
logger = logging.getLogger("data_staging")
logger.info("Logger initialized successfully")





# Create a script to programmatically download the large CSV files using requests
# Put the three files in minIO





# I have both of these pulling from an API so the data is as up to date as possible 

# Combined function: Download payroll data, save as CSV, upload to MinIO, and load to Snowflake
# Download as a .json
def process_payroll_data(minio_client, bucket_name, object_name="payroll_data.csv", conn=None, snowflake_db=None, snowflake_schema=None):
    QUERY_URL = os.getenv('PAY_ROLL_ENDPOINT')
    try:
        logger.info(f"Downloading payroll data from {QUERY_URL} using $limit and $offset for all rows")
        all_data = []
        offset = 0
        limit = 50000
        while True:
            paged_url = f"{QUERY_URL}?$limit={limit}&$offset={offset}"
            logger.info(f"Fetching rows {offset} to {offset+limit} from {paged_url}")
            response = requests.get(paged_url)
            response.raise_for_status()
            data = response.json()
            if not data:
                break
            all_data.extend(data)
            if len(data) < limit:
                break
            offset += limit

        df = pd.DataFrame(all_data)
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_bytes = io.BytesIO(csv_buffer.getvalue().encode())
        csv_size = csv_bytes.getbuffer().nbytes

        # Upload to MinIO from memory
        csv_bytes.seek(0)
        minio_client.put_object(
            bucket_name,
            object_name,
            csv_bytes,
            csv_size,
            content_type="text/csv"
        )
        logger.info(f"Payroll data uploaded to MinIO bucket '{bucket_name}' as '{object_name}'")

        # If Snowflake connection and schema info provided, load to Snowflake
        if conn and snowflake_db and snowflake_schema:
            logger.info(f"Loading payroll data from CSV to Snowflake: {snowflake_db}.{snowflake_schema}")
            df["SOURCE_FILE"] = object_name
            df["LOAD_TIMESTAMP_UTC"] = datetime.datetime.now(datetime.timezone.utc)
            df.columns = df.columns.str.upper()
            success, nchunks, nrows, _ = write_pandas(
                conn=conn,
                df=df,
                database=snowflake_db,
                schema=snowflake_schema,
                auto_create_table=True,
                overwrite=True,
                quote_identifiers=True,
                use_logical_type=True
            )
            if success:
                logger.info(f"Data successfully written to Snowflake table {snowflake_db}.{snowflake_schema} - {nrows} rows in {nchunks} chunks.")
            else:
                logger.error(f"Failed to write data to Snowflake table with write_pandas.")
    except S3Error as e:
        logger.error(f"Minio S3 error: {e}")
    except pd.errors.EmptyDataError as e:
        logger.error(f"Empty data error: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")


# Job postings: export to a csv and store in minIO
# Download as a csv

def process_job_posting_data(minio_client, bucket_name, object_name="job_posting_data.csv", conn=None, snowflake_db=None, snowflake_schema=None):
    QUERY_URL = os.getenv('JOB_POSTING_ENDPOINT')
    try:
        logger.info(f"Downloading job posting CSV from {QUERY_URL}")

        response = requests.get(QUERY_URL)
        response.raise_for_status()
        csv_bytes = io.BytesIO(response.content)
        csv_size = len(response.content)

        # Upload to MinIO from memory
        csv_bytes.seek(0)
        minio_client.put_object(
            bucket_name,
            object_name,
            csv_bytes,
            csv_size,
            content_type="text/csv"
        )
        logger.info(f"Job posting data uploaded to MinIO bucket '{bucket_name}' as '{object_name}'")

        # If Snowflake connection and schema info provided, load to Snowflake
        if conn and snowflake_db and snowflake_schema:
            logger.info(f"Loading job posting data from CSV to Snowflake: {snowflake_db}.{snowflake_schema}")
            csv_bytes.seek(0)
            df = pd.read_csv(csv_bytes)
            df["SOURCE_FILE"] = object_name
            df["LOAD_TIMESTAMP_UTC"] = datetime.datetime.now(datetime.timezone.utc)
            df.columns = df.columns.str.upper()
            success, nchunks, nrows, _ = write_pandas(
                conn=conn,
                df=df,
                database=snowflake_db,
                schema=snowflake_schema,
                auto_create_table=True,
                overwrite=True,
                quote_identifiers=True,
                use_logical_type=True
            )
            if success:
                logger.info(f"Data successfully written to Snowflake table {snowflake_db}.{snowflake_schema} - {nrows} rows in {nchunks} chunks.")
            else:
                logger.error(f"Failed to write data to Snowflake table with write_pandas.")
    except S3Error as e:
        logger.error(f"Minio S3 error: {e}")
    except pd.errors.EmptyDataError as e:
        logger.error(f"Empty data error: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")

# Lighthouse Data: XLSX file transformed into a csv and dropped into minIO


def process_lighthouse_data(minio_client, bucket_name, local_xlsx_path, object_name="lighthouse_data.csv"):
    """
    Convert a local XLSX file to CSV and upload it to MinIO.
    Args:
        minio_client: MinIO client instance
        bucket_name: MinIO bucket name
        local_xlsx_path: Path to the local XLSX file
        object_name: Name for the CSV file in MinIO
    """
    try:
        logger.info(f"Reading Excel file from {local_xlsx_path}")
        df = pd.read_excel(local_xlsx_path)
        local_csv_path = os.path.join(LOG_DIR, object_name)
        df.to_csv(local_csv_path, index=False)
        logger.info(f"Converted Excel to CSV at {local_csv_path}")

        # Upload to MinIO
        with open(local_csv_path, "rb") as file_data:
            file_stat = os.stat(local_csv_path)
            minio_client.put_object(
                bucket_name,
                object_name,
                file_data,
                file_stat.st_size,
                content_type="text/csv"
            )
        logger.info(f"Lighthouse data uploaded to MinIO bucket '{bucket_name}' as '{object_name}'")
    except Exception as e:
        logger.error(f"Failed to process or upload lighthouse data: {e}")



# Main function to orchestrate the data processing
# Connect to snowflake
def main(): 
    
    MINIO_EXTERNAL_URL = os.getenv('MINIO_EXTERNAL_URL')
    MINIO_BUCKET_NAME = os.getenv('MINIO_BUCKET_NAME')
    MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY')
    MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY')
    SNOWFLAKE_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
    SNOWFLAKE_USER = os.getenv('SNOWFLAKE_USER')
    SNOWFLAKE_PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
    SNOWFLAKE_DATABASE = os.getenv('SNOWFLAKE_DATABASE')
    SNOWFLAKE_SCHEMA_BRONZE = os.getenv('SNOWFLAKE_SCHEMA_BRONZE')
    SNOWFLAKE_WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE')
    SNOWFLAKE_ROLE = os.getenv('SNOWFLAKE_ROLE')

    minio_client = None
    conn = None


    try:
        minio_client = Minio(
            MINIO_EXTERNAL_URL,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False
        )
        logger.info("Connected to MinIO successfully.")
    except S3Error as e:
        logger.error(f"Error connecting to MinIO: {e}")
        raise

    # Download and upload payroll data to MinIO if not already present

    found = False
    for obj in minio_client.list_objects(MINIO_BUCKET_NAME, recursive=True):
        if obj.object_name == "payroll_data.csv":
            found = True
            break
    if not found:
        process_payroll_data(minio_client, MINIO_BUCKET_NAME, "payroll_data.csv")
    else:
        logger.info("payroll_data.csv already exists in MinIO bucket.")

    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA_BRONZE,
        warehouse=SNOWFLAKE_WAREHOUSE,
        role=SNOWFLAKE_ROLE
    )
    sf_cursor = conn.cursor()
    logger.info("Connected to Snowflake successfully.")



    # Optionally, process and load payroll data to Snowflake
    # process_payroll_data(
    #     minio_client,
    #     MINIO_BUCKET_NAME,
    #     "payroll_data.csv",
    #     conn=conn,
    #     snowflake_db=SNOWFLAKE_DATABASE,
    #     snowflake_schema=SNOWFLAKE_SCHEMA_BRONZE
    # )

    # Process and load job posting data to MinIO and Snowflake
    process_job_posting_data(
        minio_client,
        MINIO_BUCKET_NAME,
        "job_posting_data.csv",
        conn=conn,
        snowflake_db=SNOWFLAKE_DATABASE,
        snowflake_schema=SNOWFLAKE_SCHEMA_BRONZE
    )

    # Process and upload Lighthouse Excel data to MinIO
    # QUERY_URL = os.getenv('JOB_POSTING_ENDPOINT'
    LIGHTHOUSE_XLSX_PATH = os.getenv('LIGHTHOUSE_XLSX_PATH')
    process_lighthouse_data(
        minio_client,
        MINIO_BUCKET_NAME,
        LIGHTHOUSE_XLSX_PATH,
        object_name="lighthouse_data.csv"
    )
    if 'sf_cursor' in locals() and sf_cursor:
        sf_cursor.close()
    if conn:
        conn.close()
    logger.info("Snowflake connection closed.")



if __name__ == "__main__":
    main()
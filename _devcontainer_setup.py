import os
import subprocess
import time
import logging
import configparser
import sys
import datetime 


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", "/opt/airflow")
AIRFLOW_CFG_PATH = os.path.join(AIRFLOW_HOME, "airflow.cfg")

def get_airflow_executable_path():
    user_bin_path = os.path.join(os.path.expanduser("~airflow"), ".local", "bin")
    airflow_exec_path = os.path.join(user_bin_path, "airflow")
    if os.path.exists(airflow_exec_path):
        return airflow_exec_path
    logger.warning(f"Airflow executable not found at {airflow_exec_path}. Trying default PATH.")
    return "airflow"

AIRFLOW_EXEC = get_airflow_executable_path()


def run_command(command, cwd=None, env=None, check=True):
    logger.info(f"Running command: {' '.join(command)}")
    process = subprocess.run(command, cwd=cwd, env=env, capture_output=True, text=True)
    if check and process.returncode != 0:
        logger.error(f"Command failed with exit code {process.returncode}")
        logger.error(f"STDOUT: {process.stdout}")
        logger.error(f"STDERR: {process.stderr}")
        raise subprocess.CalledProcessError(process.returncode, command, process.stdout, process.stderr)
    else:
        logger.info(f"STDOUT: {process.stdout}")
        if process.stderr:
            logger.warning(f"STDERR: {process.stderr}")
    return process.stdout

def modify_airflow_cfg(section, key, value):
    logger.info(f"Modifying airflow.cfg: [{section}] {key} = {value}")
    config = configparser.ConfigParser()
    
    if os.path.exists(AIRFLOW_CFG_PATH):
        config.read(AIRFLOW_CFG_PATH)
    else:
        logger.warning(f"airflow.cfg not found at {AIRFLOW_CFG_PATH}. It will be created or read after db migrate.")

    if section not in config:
        config[section] = {}
    
    config[section][key] = value

    with open(AIRFLOW_CFG_PATH, 'w') as configfile:
        config.write(configfile)
    logger.info(f"Successfully updated airflow.cfg: [{section}] {key} = {value}")


def main():
    logger.info("Starting Dev Container Airflow setup (core configuration only)...")

    os.environ['AIRFLOW_HOME'] = AIRFLOW_HOME

    # Adjust permissions (ensure airflow user owns mounted volumes)
    logger.info("Adjusting permissions for mounted volumes...")
    try:
        run_command(["sudo", "chown", "-R", "airflow:airflow", os.path.join(AIRFLOW_HOME, 'airflow')], check=False)
        run_command(["sudo", "chmod", "-R", "775", os.path.join(AIRFLOW_HOME, 'airflow')], check=False)
        logger.info("Permissions adjusted.")
    except Exception as e:
        logger.error(f"Failed to adjust permissions: {e}. This might cause issues.")


    # Set SQLite Metadata DB Connection (or Postgres if configured in .env)
    sql_alchemy_conn = os.getenv('AIRFLOW__DATABASE__SQL_ALCHEMY_CONN')
    if not sql_alchemy_conn: # If env var not set, default to SQLite
        POSTGRES_USER = os.getenv("POSTGRES_USER")
        POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
        POSTGRES_DB = os.getenv("POSTGRES_DB")
        POSTGRES_HOST = os.getenv("POSTGRES_HOST")
        if POSTGRES_USER and POSTGRES_PASSWORD and POSTGRES_DB and POSTGRES_HOST:
            sql_alchemy_conn = f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:5432/{POSTGRES_DB}"
        else:
            sql_alchemy_conn = f"sqlite:////{AIRFLOW_HOME}/airflow.db"
            logger.warning("No PostgreSQL env vars found. Defaulting to SQLite for database connection.")
    os.environ['AIRFLOW__DATABASE__SQL_ALCHEMY_CONN'] = sql_alchemy_conn # Set for db migrate and config write


    # Initialize Airflow Database
    logger.info(f"Initializing Airflow database using: {sql_alchemy_conn} ...")
    run_command([AIRFLOW_EXEC, "db", "migrate"], cwd=AIRFLOW_HOME, env=os.environ.copy())
    logger.info("Airflow database migrated.")

    # Finalize airflow.cfg settings
    logger.info("Setting Airflow Executor in config...")
    modify_airflow_cfg("core", "executor", "SequentialExecutor") # As per your last decision

    new_dags_folder_path = os.path.join(AIRFLOW_HOME, 'airflow', 'dags')
    logger.info(f"Setting Airflow dags_folder to: {new_dags_folder_path}")
    modify_airflow_cfg("core", "dags_folder", new_dags_folder_path)

    logger.info("Disabling Airflow example DAGs in config...")
    modify_airflow_cfg("core", "load_examples", "False")

    logger.info("Setting SQL_ALCHEMY_CONN in airflow.cfg for persistence...")
    modify_airflow_cfg("database", "sql_alchemy_conn", sql_alchemy_conn)


    # Create Airflow User
    logger.info("Creating Airflow user...")
    admin_username = os.getenv("_AIRFLOW_WWW_USER_USERNAME", "airflow")
    admin_password = os.getenv("_AIRFLOW_WWW_USER_PASSWORD", "airflow")
    admin_email = "airflow@example.com"
    
    try:
        run_command([
            AIRFLOW_EXEC, "users", "create",
            "--username", admin_username,
            "--firstname", "Airflow",
            "--lastname", "User",
            "--role", "Admin",
            "--email", admin_email,
            "--password", admin_password
        ], cwd=AIRFLOW_HOME, env=os.environ.copy())
        logger.info(f"Airflow user '{admin_username}' created.")
    except subprocess.CalledProcessError as e:
        if "already exists" in e.stderr:
            logger.warning(f"Airflow user '{admin_username}' already exists. Skipping creation.")
        else:
            raise

    logger.info("Airflow core setup complete. Webserver and Scheduler will be started manually.")

if __name__ == "__main__":
    main()
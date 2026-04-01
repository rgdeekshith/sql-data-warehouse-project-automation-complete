import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

def get_universal_engine():
    """
    Creates a universal database connection engine using SQLAlchemy.
    It reads the generic DATABASE_URL from the .env file.
    """
    load_dotenv()
    db_url = os.getenv('DATABASE_URL')
    
    if not db_url:
        raise ValueError("DATABASE_URL is missing in the .env file.")
    
    # create_engine dynamically understands which database to talk to based on the URL
    return create_engine(db_url)

def load_csv_to_bronze(engine, datasets_path):
    """
    Universally loads CSVs into the Bronze schema using Pandas.
    This replaces database-specific commands like BULK INSERT or COPY.
    """
    print("\n🥉 Ingesting Bronze Layer...")
    
    # Define the files and their target table names
    files_to_load = {
        'crm_cust_info': os.path.join(datasets_path, 'source_crm', 'cust_info.csv'),
        'erp_cust_az12': os.path.join(datasets_path, 'source_erp', 'CUST_AZ12.csv')
        # Add the remaining 4 files here following the same pattern
    }

    for table_name, file_path in files_to_load.items():
        if os.path.exists(file_path):
            print(f"   -> Loading {file_path} into bronze.{table_name}")
            # Read CSV into a Pandas DataFrame
            df = pd.read_csv(file_path)
            
            # Universally push the DataFrame to the database.
            # 'replace' automatically drops and recreates the table for a fresh load.
            df.to_sql(name=table_name, con=engine, schema='bronze', if_exists='replace', index=False)
        else:
            print(f"   -> ⚠️ Missing file: {file_path}")

def execute_sql_file(engine, filepath):
    """
    Executes a standard SQL file containing transformation logic.
    """
    if not os.path.exists(filepath):
        print(f"⚠️ Warning: {filepath} not found.")
        return

    with open(filepath, 'r') as file:
        sql_script = file.read()

    # Split script by standard semicolons instead of SQL Server's 'GO' keyword
    sql_statements = sql_script.split(';')

    # Connect to the engine and execute statements within a transaction
    with engine.begin() as connection:
        for statement in sql_statements:
            if statement.strip():
                connection.execute(text(statement))

def run_pipeline():
    """
    Orchestrates the Bronze, Silver, and Gold execution in order.
    """
    current_dir = os.path.dirname(os.path.abspath(__file__))
    datasets_path = os.path.join(current_dir, 'datasets')
    sql_scripts_dir = os.path.join(current_dir, 'sql_scripts')

    try:
        print("🚀 Starting Universal Medallion Pipeline...")
        
        # 1. Establish universal connection
        engine = get_universal_engine()

        # 2. Ensure schemas exist (creating schemas varies slightly by DB, 
        # but this simple DDL works for Postgres/SQL Server)
        with engine.begin() as conn:
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS bronze;"))
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS silver;"))
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS gold;"))

        # 3. Bronze Layer (Python/Pandas replaces SQL here)
        load_csv_to_bronze(engine, datasets_path)

        # 4. Silver Layer (Clean & Standardize)
        print("\n🥈 Transforming Silver Layer...")
        execute_sql_file(engine, os.path.join(sql_scripts_dir, '01_silver_layer.sql'))

        # 5. Gold Layer (Business Logic)
        print("\n🥇 Modeling Gold Layer...")
        execute_sql_file(engine, os.path.join(sql_scripts_dir, '02_gold_layer.sql'))

        print("\n✅ Pipeline Executed Successfully!")

    except Exception as e:
        print(f"\n❌ Pipeline Error: {e}")

if __name__ == "__main__":
    run_pipeline()

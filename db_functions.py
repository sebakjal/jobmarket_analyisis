import polars as pl
import duckdb
from pathlib import Path # For easier path handling


def create_table(df: pl.DataFrame, 
                 table_name: str, 
                 if_exists_strategy: str,
                 db_file: str,
                 dataset: str):
    """
    Function to create a table in DuckDB from a Polars DataFrame.
    
    Parameters:
    df (pl.DataFrame): The Polars DataFrame to write to DuckDB.
    table_name (str): The name of the table to create in DuckDB.
    if_exists_strategy (str): Strategy for handling existing tables ('fail', 'replace', 'append').
    db_file (str): Path to the DuckDB database file.
    """

    # --- 3. Write DataFrame to DuckDB using Native API ---
    print(f"\nAttempting to write DataFrame to table '{table_name}' in '{db_file}'")
    print(f"Strategy if table exists: '{if_exists_strategy}'")

    try:
        # Connect to DuckDB. Creates the file if it doesn't exist.
        # Using 'with' ensures the connection is closed automatically
        with duckdb.connect(database=str(db_file), read_only=False) as conn:
            print(f"Connected to DuckDB file: {db_file}")

            # Register the Polars DataFrame as a temporary view in DuckDB
            # This is often zero-copy or highly optimized
            conn.register('df_view', df)
            print("Polars DataFrame registered as 'df_view'.")

            # --- Execute SQL based on the strategy ---
            if if_exists_strategy == 'fail':
                # Check if table exists first
                check_sql = f"SELECT 1 FROM information_schema.tables WHERE table_name = '{table_name}' LIMIT 1;"
                if conn.execute(check_sql).fetchone():
                    raise ValueError(f"Table '{table_name}' already exists and if_exists is 'fail'.")
                else:
                    # Create and insert
                    sql = f"CREATE TABLE {table_name} AS SELECT * FROM df_view;"
                    conn.execute(sql)
                    print(f"Executed: {sql}")

            elif if_exists_strategy == 'replace':
                # Drop if exists and recreate
                sql = f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM df_view;"
                conn.execute(sql)
                print(f"Executed: {sql}")

            elif if_exists_strategy == 'append':
                # Create table if it doesn't exist based on the DataFrame schema
                # DuckDB automatically infers schema from the view
                sql_create = f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM df_view LIMIT 0;"
                conn.execute(sql_create)
                print(f"Executed: {sql_create} (if table didn't exist)")

                # Insert the data
                sql_insert = f"INSERT INTO {table_name} SELECT * FROM df_view;"
                conn.execute(sql_insert)
                print(f"Executed: {sql_insert}")

            else:
                raise ValueError(f"Invalid if_exists_strategy: {if_exists_strategy}")

            # Optional: Unregister the view if you want to clean up immediately
            conn.unregister('df_view')

            print(f"\nSuccessfully wrote data to table '{table_name}'.")

    except duckdb.Error as e:
        print(f"\n--- DuckDB ERROR ---")
        print(f"An error occurred: {e}")
    except ValueError as e:
        print(f"\n--- ERROR ---")
        print(f"An error occurred: {e}")
    except Exception as e:
        print(f"\n--- UNEXPECTED ERROR ---")
        print(f"An unexpected error occurred: {e}")

    return None

import os # For potential database file deletion in example

def insert_into_db(df: pl.DataFrame,
                   table_name: str,
                   db_file: str,
                   dataset: str):
    
    # --- Configuration ---
    key_column = "job_url" # IMPORTANT: Set your unique key column name here

    # --- Connect to DuckDB ---
    conn = duckdb.connect(database=db_file, read_only=False)

    # --- Upsert Logic ---
    print(f"\n--- Performing UPSERT for table '{table_name}' on key '{key_column}' ---")

    # 1. Register the DataFrame to upsert as a view
    view_name = "df_upsert_view" # Use a unique view name
    conn.register(view_name, df)
    print(f"Registered DataFrame as view '{view_name}'")

    # 2. Ensure the target table exists (create if not)
    #    This uses the schema from the view/DataFrame
    #    LIMIT 0 creates the structure without inserting data initially
    if dataset == 'base_data':
        sql_create_if_not_exists = f"""CREATE TABLE IF NOT EXISTS {table_name}(
                                    title VARCHAR,          
                                    company VARCHAR,
                                    location VARCHAR,
                                    date DATE,
                                    job_url VARCHAR PRIMARY KEY,
                                    job_description VARCHAR,
                                    seniority_level VARCHAR,
                                    employment_type VARCHAR,
                                    job_function VARCHAR,
                                    industries VARCHAR,
                                    );
                                    """
    elif dataset == 'genai_data':
        sql_create_if_not_exists = f"""CREATE TABLE IF NOT EXISTS {table_name}(
                                    job_url VARCHAR PRIMARY KEY,
                                    task_clarity VARCHAR,
                                    seniority_level_ai VARCHAR,
                                    requires_degree_it VARCHAR,
                                    mentions_certifications VARCHAR,
                                    years_of_experience VARCHAR,
                                    is_in_english VARCHAR,
                                    cloud_preference VARCHAR,
                                    skills_mentioned VARCHAR[]
                                    );
                                    """        
    conn.execute(sql_create_if_not_exists)
    print(f"Executed: {sql_create_if_not_exists} (if table didn't exist)")

    # 3. Construct the UPSERT SQL statement
    all_columns = df.columns
    columns_to_update = [col for col in all_columns if col != key_column]

    if not columns_to_update:
        print("Warning: No columns to update specified (only key column found). Performing INSERT only.")
        # Fallback to simple insert if only key column exists
        sql_upsert = f"""
            INSERT INTO {table_name} ({', '.join(all_columns)})
            SELECT {', '.join(all_columns)} FROM {view_name}
            ON CONFLICT ({key_column}) DO NOTHING;
        """
    else:
        # Prepare column lists for SQL
        all_columns_sql = ', '.join(f'"{c}"' for c in all_columns) # Quote column names
        update_setters = ', '.join([f'"{col}" = excluded."{col}"' for col in columns_to_update])

        sql_upsert = f"""
        INSERT INTO {table_name} ({all_columns_sql})
        SELECT {all_columns_sql} FROM {view_name}
        ON CONFLICT ({key_column}) DO UPDATE SET
            {update_setters};
        """
        # The `excluded` keyword refers to the row that *failed* to be inserted
        # due to the conflict, allowing you to use its values in the UPDATE.

    print(f"\nExecuting UPSERT SQL:\n{sql_upsert}")
    try:
        conn.execute(sql_upsert)
        print("UPSERT execution successful.")
    except Exception as e:
        print(f"Error during UPSERT execution: {e}")

    # 4. Clean up the temporary view
    conn.unregister(view_name)
    print(f"Unregistered view '{view_name}'")

    # --- Close Connection ---
    conn.close()
    print("\nConnection closed.")
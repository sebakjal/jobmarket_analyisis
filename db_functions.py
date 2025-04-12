import polars as pl
import duckdb
from pathlib import Path # For easier path handling


def create_table(df: pl.DataFrame, 
                 table_name: str, 
                 if_exists_strategy: str,
                 db_file: str):
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
            # conn.unregister('df_view')

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
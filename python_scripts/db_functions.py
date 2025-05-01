import polars as pl
import duckdb

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
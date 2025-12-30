import psycopg2
import os

def create_database_schema():
    """
    Connects to the PostgreSQL database, drops any existing table,
    and creates a new, clean table with the correct OHLC schema.
    """
    conn = None
    try:
        # --- Using your credentials ---
        DB_HOST = os.getenv("DB_HOST", "localhost")
        DB_NAME = os.getenv("POSTGRES_DB", "quant_data_db")
        DB_USER = os.getenv("POSTGRES_USER", "mashooqrabbanishaik")
        DB_PASS = os.getenv("POSTGGRES_PASSWORD", "quant@300")

        # Connect to your PostgreSQL database
        conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS)
        cur = conn.cursor()

       # Drop the old table  (because im crating new table with OHLC)---
        # This is necessary because the table already exists with the old schema.
        print("Dropping existing 'intraday_data' table (if it exists)...")
        cur.execute("DROP TABLE IF EXISTS intraday_data;")

        #  Creates the new table from scratch ---
        print("Creating new 'intraday_data' table with OHLC schema...")
        cur.execute("""
            CREATE TABLE intraday_data (
                id SERIAL PRIMARY KEY,
                symbol VARCHAR(10) NOT NULL,
                timestamp TIMESTAMP WITHOUT TIME ZONE NOT NULL,
                open NUMERIC(15, 4) NOT NULL,
                high NUMERIC(15, 4) NOT NULL,
                low NUMERIC(15, 4) NOT NULL,
                close NUMERIC(15, 4) NOT NULL,
                volume INTEGER,
                spread NUMERIC(15, 4),
                z_score NUMERIC(15, 4),
                
                -- Prevents duplicate entries for the same stock at the same time
                UNIQUE(symbol, timestamp) 
            );
        """)

        # Commit the transaction to save the changes
        conn.commit()
        
        print("\nDatabase schema successfully created.")
        cur.close()

    except Exception as e:
        print(f"Error creating database schema: {e}")
        if conn:
            conn.rollback() # Rollback changes if an error occurs
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    create_database_schema()
    
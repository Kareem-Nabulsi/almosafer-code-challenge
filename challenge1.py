from sqlalchemy import create_engine, text
import pandas as pd

# Database connection details
DB_NAME = "customer_data"
DB_USER = "postgres"
DB_PASSWORD = ""
DB_HOST = "localhost"
DB_PORT = "5432"

# Create connection string
connection_string = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Validation queries
QUERIES = {
    "Duplicated": """
        SELECT first_name, last_name, email, phone_number, COUNT(*) AS duplicate_count
        FROM customer_table
        GROUP BY first_name, last_name, email, phone_number
        HAVING COUNT(*) > 1;
    """,
    "Missing Data": """
        SELECT *
        FROM customer_table
        WHERE first_name IS NULL
        OR last_name IS NULL
        OR address IS NULL
        OR city IS NULL
        OR state IS NULL
        OR zip_code IS NULL
        OR phone_number IS NULL
        OR email IS NULL
        OR birthdate IS NULL;
    """,
    "Invalid Emails Formats": """
        SELECT id, email
        FROM customer_table
        WHERE email NOT LIKE '%@%.%';
    """,
    "Invalid Phone Number Formats": """
        SELECT id, phone_number
        FROM customer_table
        WHERE phone_number NOT LIKE '___-___-____';
    """,
    "Inconsistent_Data": """
        SELECT first_name, last_name, COUNT(DISTINCT email) AS unique_emails, COUNT(DISTINCT address) AS unique_addresses
        FROM customer_table
        GROUP BY first_name, last_name
        HAVING COUNT(DISTINCT email) > 1 OR COUNT(DISTINCT address) > 1;
    """
}

try:
    # Create engine to connect to PostgreSQL
    engine = create_engine(connection_string)

    # Open a connection to the database
    with engine.connect() as connection:
        # Create a Pandas excel writer to save all results in one file
        with pd.ExcelWriter("Challenge One - Results.xlsx", engine="xlsxwriter") as writer:
            for query_name, query in QUERIES.items():
                # Execute each query
                result = connection.execute(text(query))
                
                # Fetch the results and convert to DataFrame
                data = result.fetchall()
                df = pd.DataFrame(data, columns=result.keys())

                # Write the DataFrame to a new sheet inside the excel file
                df.to_excel(writer, sheet_name=query_name, index=False)

            print("Results have been saved to: Challenge One - Results.xlsx")

except Exception as e:
    print(f"Error occured: {e}")
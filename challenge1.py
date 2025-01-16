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
    """,
    "Invalid Birthdate Formats": """
        SELECT id, birthdate
        FROM customer_table
        WHERE birthdate IS NOT NULL AND birthdate !~ '^\\d{4}-\\d{2}-\\d{2}$';
    """,
    "Invalid Zip Codes": """
        SELECT id, zip_code
        FROM customer_table
        WHERE zip_code IS NOT NULL AND zip_code !~ '^\\d{5}$';
    """,
    "Invalid States": """
        SELECT id, state
        FROM customer_table
        WHERE state IS NOT NULL AND state NOT IN (
            'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 
            'ME', 'MD', 'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ', 'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 
            'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY'
        );
    """,
    "Invalid Addresses": """
        SELECT id, address
        FROM customer_table
        WHERE address IS NULL OR address !~ '[A-Za-z0-9]';
    """,
    "Security Validation": """
        SELECT id, first_name, last_name, email, phone_number, address
        FROM customer_table
        WHERE LENGTH(first_name) > 50
        OR LENGTH(last_name) > 50
        OR LENGTH(email) > 100
        OR LENGTH(phone_number) > 20
        OR LENGTH(address) > 200;
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

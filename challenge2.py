import pandas as pd
from sqlalchemy import create_engine, text

# Database connection details
DB_NAME = "customer_data"
DB_USER = "postgres"
DB_PASSWORD = ""
DB_HOST = "localhost"
DB_PORT = "5432"

# Create connection
connection_string = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"



# Create the database tables:

"""

# Load data from Excel file
excel_file = 'QA code challange Question 2 - Services .xlsx'

# Read both sheets into separate DataFrames
df_sheet1 = pd.read_excel(excel_file, sheet_name='l1_services')
df_sheet2 = pd.read_excel(excel_file, sheet_name='l2_services')

# Create the tables in the database:
df_sheet1.to_sql('l1', connection_string, if_exists='replace', index=False)
df_sheet2.to_sql('l2', connection_string, if_exists='replace', index=False)

print("Tables created successfully.")

"""

# SQL queries to fetch the data from L1 and L2 tables
L1_query = "SELECT * FROM l1;"
L2_query = "SELECT * FROM l2;"


# Execute the queries and fetch the results into DataFrames
try:
    # Create engine to connect to PostgreSQL
    engine = create_engine(connection_string)

    # Open a connection to the database
    with engine.connect() as connection:

        # Fetch data from L1 and L2 tables

        # Fetch data from L1
        L1 = connection.execute(text(L1_query))
        # Fetch all the results and convert them into a DataFrame
        allResults = L1.fetchall()
        L1 = pd.DataFrame(allResults, columns=L1.keys())

        # Fetch data from L2
        L2 = connection.execute(text(L2_query))
        # Fetch all the results and convert them into a DataFrame
        allResults = L2.fetchall()
        L2 = pd.DataFrame(allResults, columns=L2.keys())

        required_business_columns = [
            'order_type', 'dim_group_id', 'order_no', 'dim_bookingdate_id', 'dim_store_id',
            'service_fee_code', 'dim_customer_id', 'dim_language', 'dim_totals_currency', 
            'dim_status_id', 'phone', 'payment_amount', 'discount_amount', 'service_fee_amount',
            'base_amount', 'inputvat', 'outputvat', 'product_vat', 'selling_price', 'selling_price_vat',
            'ibv', 'iov_usd', 'gbv', 'gbv_usd'
        ]

        # Filter out rows where product_type != order_type
        filtered_L1 = L1[L1['product_type'] == L1['order_type']]

        # Initialize transformed L1
        transformed_L1 = []

        # Transform filtered_L1 row by row based on business rules
        for _, row_L1 in filtered_L1.iterrows():
            # Apply transformations
            transformed_row = {
                'order_type': row_L1['order_type'],
                'dim_group_id': row_L1['dim_group_id'],
                'order_no': row_L1['order_no'],
                'dim_bookingdate_id': row_L1['dim_bookingdate_id'],
                'dim_store_id': row_L1['dim_store_id'],
                'service_fee_code': row_L1['product_name'] if row_L1['product_type'] == 'rule' else None,
                'dim_customer_id': row_L1['dim_customer_id'],
                'dim_language': row_L1['dim_language'],
                'dim_totals_currency': row_L1['dim_totals_currency'],
                'dim_status_id': row_L1['dim_status_id'],
                'phone': row_L1['phone'],
                'payment_amount': row_L1['payment_amount'],
                'discount_amount': row_L1['discount_amount'],
                'service_fee_amount': row_L1['service_fee_amount'],
                'base_amount': row_L1['base_amount'],
                'inputvat': row_L1['inputvat'],
                'outputvat': row_L1['outputvat'],
                'product_vat': row_L1['product_vat'],
                'selling_price': row_L1['selling_price'],
                'selling_price_vat': row_L1['selling_price_vat'],
                'ibv': row_L1['ibv'],
                'iov_usd': row_L1['ibv'] * row_L1['conversion_rate_usd'],
                'gbv': row_L1['gbv'],
                'gbv_usd': row_L1['gbv'] * row_L1['conversion_rate_usd'],
            }
            transformed_L1.append(transformed_row)

        # Convert transformed rows into a DataFrame
        transformed_L1_df = pd.DataFrame(transformed_L1)

        # Compare transformed_L1_df to L2 and collect mismatched rows

        errors = []

        # Function to check if values are different (with tolerance for floating-point numbers)
        def are_values_different(val1, val2):
            # Function to remove .0 from strings if present
            def clean_string(value):
                if isinstance(value, str) and value.endswith('.0'):
                    return value[:-2]  # Remove the last 2 characters (".0")
                return value

            # Clean both values before further checks
            val1 = clean_string(val1)
            val2 = clean_string(val2)

            # Check for None or NaN values
            if val1 is None or val2 is None:
                return val1 is not val2  # True if one is None and the other is not

            # Handle NaN values explicitly using pd.isna()
            if pd.isna(val1) or pd.isna(val2):
                return pd.isna(val1) != pd.isna(val2)  # If one is NaN and the other is not, they're different
            
            # Handle numerical values (int or float)
            if isinstance(val1, (int, float)) and isinstance(val2, (int, float)):
                return abs(val1 - val2) >= 0.6  # Consider it a mismatch if the difference is 0.6 or greater, (took the absolute value)
            
            # If they are both strings, we compare them directly
            if isinstance(val1, str) and isinstance(val2, str):
                return val1 != val2
            
            # For non-numeric (strings, datetime, etc.), check exact equality
            return str(val1) != str(val2)

        # Iterate over rows in L2
        for _, row_L2 in L2.iterrows():
            # Find matching row in transformed_L1_df based on 'order_no'
            matching_rows = transformed_L1_df[transformed_L1_df['order_no'] == row_L2['order_no']]
            
            if matching_rows.empty:
                # No matching row found in L1
                errors.append({'order_no': row_L2['order_no'], 'error': 'No matching row in L1'})
            else:
                # If a matching row is found, compare each column
                for col in matching_rows.columns:
                    value_L2 = str(row_L2[col])
                    value_L1 = str(matching_rows.iloc[0][col])

                    if are_values_different(value_L1, value_L2):
                        errors.append({
                            'order_no': row_L2['order_no'],
                            'column': col,
                            'L1_value': value_L1,
                            'L2_value': value_L2
                        })

        # Find columns in L1 that are missing in L2
        missing_in_L2 = set(L1.columns) - set(L2.columns)
        
        
        # Find extra columns in L1 compared to L2
        if missing_in_L2:
            for col in missing_in_L2:
                if col in required_business_columns:
                    errors.append({'error': f"Column '{col}' is missing in L2"})
        
        # If there are errors, save them to an Excel file
        if errors:
            errors_df = pd.DataFrame(errors)
            errors_df.to_excel('Challenge Two - Results.xlsx', index=False)
            print(f"Found {len(errors)} mismatches. Errors saved to 'Challenge Two - Results.xlsx'.")
        else:
            print("No mismatches found.")

except Exception as e:
    print(f"Error occurred: {e}")

import psycopg2

def check_quality_control(connection):
    cursor = connection.cursor()

    # Check unexpected strings
    cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'your_table_name'")
    columns = cursor.fetchall()

    for column in columns:
        column_name = column[0]
        cursor.execute(f"SELECT DISTINCT {column_name} FROM your_table_name WHERE {column_name} NOT SIMILAR TO 'expected_regex_pattern'")
        unexpected_strings = cursor.fetchall()
        if unexpected_strings:
            print(f"Unexpected strings found in column '{column_name}':")
            for unexpected_string in unexpected_strings:
                print(unexpected_string[0])

    # Check unexpected numerical values
    cursor.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'your_table_name' AND data_type IN ('integer', 'float', 'numeric')")
    numeric_columns = cursor.fetchall()

    for column in numeric_columns:
        column_name = column[0]
        cursor.execute(f"SELECT DISTINCT {column_name} FROM your_table_name WHERE {column_name} < expected_minimum_value OR {column_name} > expected_maximum_value")
        unexpected_values = cursor.fetchall()
        if unexpected_values:
            print(f"Unexpected numerical values found in column '{column_name}':")
            for unexpected_value in unexpected_values:
                print(unexpected_value[0])

    # Check unexpected dates
    cursor.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'your_table_name' AND data_type IN ('date', 'timestamp')")
    date_columns = cursor.fetchall()

    for column in date_columns:
        column_name = column[0]
        cursor.execute(f"SELECT DISTINCT {column_name} FROM your_table_name WHERE {column_name} < 'expected_minimum_date' OR {column_name} > 'expected_maximum_date'")
        unexpected_dates = cursor.fetchall()
        if unexpected_dates:
            print(f"Unexpected dates found in column '{column_name}':")
            for unexpected_date in unexpected_dates:
                print(unexpected_date[0])

    # Check join conditions
    cursor.execute("SELECT COUNT(*) FROM your_table_name JOIN other_table ON join_condition")
    join_result = cursor.fetchone()
    if join_result[0] == 0:
        print("Join condition is not valid. No records found.")

    cursor.close()

# Connect to PostgreSQL
connection = psycopg2.connect(host='host', port='port', database='database', user='user', password='password')

# Call the quality control function
check_quality_control(connection)

# Close the connection
connection.close()

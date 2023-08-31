def send_data_to_mysql():
    import pandas as pd
    import pymysql
    import csv
    print('send_data_to_mysql')
    # Input CSV file path
    input_file_path = "/opt/airflow/dags/data/advertising1.csv"

    # # Process data using pandas DataFrame
    # df = pd.read_csv(input_file_path)

    # # Perform your data processing operations here
    # # For example, you can use df.filter(), df.groupby(), etc.

    # # Define MySQL connection parameters
    mysql_host = "mysql"
    mysql_port = 3306
    mysql_user = "root"
    mysql_password = "password"
    mysql_database = "mysql"
    mysql_table = "advertising1"

    # # Create a connection to MySQL
    connection = pymysql.connect(
        host=mysql_host,
        port=mysql_port,
        user=mysql_user,
        password=mysql_password,
        database=mysql_database
    )

    # # Insert the processed data into MySQL table using pandas
    with connection.cursor() as cursor:
        # Truncate the existing data in the table (optional)
        # cursor.execute()
        query = f"""create table if not exists {mysql_table} (id varchar(200),
                                                            daily_time_spent varchar(200), 
                                                            age int, 
                                                            area_income decimal(10,2),
                                                            daily_internet_usage decimal(10,2),
                                                            topic varchar(200),
                                                            city varchar(200),
                                                            male int,
                                                            country varchar(200),
                                                            timestamp timestamp,
                                                            clicked int
        );"""
        cursor.execute(query)

        with open(input_file_path, "r") as csv_file:
            csv_reader = csv.reader(csv_file)
            next(csv_reader)  # Skip the header row if present

            for row in csv_reader:

                # Insert data into the table
                sql = "INSERT INTO advertising1 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                cursor.execute(sql, tuple(row))

    # Commit the changes to the database
    connection.commit()

    # Close the MySQL connection
    connection.close()

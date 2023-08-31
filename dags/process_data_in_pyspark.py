from pyspark.sql import SparkSession

def process_data_in_pyspark():
    # Create a Spark session
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, when, expr, avg
    from pyspark.sql.types import StringType

    # Create a Spark session
    spark = SparkSession.builder.appName("RetailDataTransformation").getOrCreate()

    # Read the CSV file into a DataFrame
    mysql_host = "mysql"
    mysql_port = 3306
    mysql_user = "root"
    mysql_password = "password"
    mysql_database = "mysql"
    mysql_table = "advertising1" 

    df = spark.read \
    .format("jdbc") \
    .option("url", f"jdbc:mysql://{mysql_host}:{mysql_port}/{mysql_database}") \
    .option("dbtable", mysql_table) \
    .option("user", mysql_user) \
    .option("password", mysql_password) \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .load() 

    # Transformations

    # 1) Change column names
    # column_mapping = {
    #     "Daily Time Spent on Site": "daily_time_spent",
    #     "Age": "age",
    #     "Area Income": "area_income",
    #     "Daily Internet Usage": "daily_internet_usage",
    #     "Ad Topic Line": "topic",
    #     "City": "city",
    #     "Male": "male",
    #     "Country": "country",
    #     "Timestamp": "timestamp",
    #     "Clicked on Ad": "clicked"
    # }
    # df = df.select([col(old_name).alias(new_name) for old_name, new_name in column_mapping.items()])

    # 2) Create age_level column
    df = df.withColumn("age_level", when(col("age").between(10, 20), 1)
                                .when(col("age").between(21, 30), 2)
                                .when(col("age").between(31, 40), 3)
                                .when(col("age").between(41, 50), 4)
                                .otherwise(5))

    # 3) Create area_income_level column
    avg_income = df.select(avg("area_income")).first()[0]
    df = df.withColumn("area_income_level", when(col("area_income") > avg_income, 1).otherwise(0))

    # 4) Create usage_level column
    df = df.withColumn("usage_level", when(col("daily_time_spent") / col("daily_internet_usage") < 0.2, "low")
                                    .when(col("daily_time_spent") / col("daily_internet_usage") < 0.4, "medium")
                                    .otherwise("high"))

    # 5) Separate date and time columns from timestamp
    df = df.withColumn("date", col("timestamp").cast("date"))
    # df = df.withColumn("time", expr("CAST(timestamp AS TIMESTAMP)"))

    # 6) Add a column for continent using an external dataset (assuming continents_data.csv)
    # continents_df = spark.read.csv("/opt/airflow/dags/data/continents_data.csv", header=True, inferSchema=True)
    # df = df.join(continents_df, on="country", how="left")

    # Establish MySQL connection
    mysql_host = "mysql"
    mysql_port = 3306
    mysql_user = "root"
    mysql_password = "password"
    mysql_database = "mysql"
    mysql_table = "advertising_transformed"

    # Write the transformed data to MySQL
    df.write \
        .format("jdbc") \
        .mode("append") \
        .option("url", f"jdbc:mysql://{mysql_host}:{mysql_port}/{mysql_database}") \
        .option("dbtable", mysql_table) \
        .option("user", mysql_user) \
        .option("password", mysql_password) \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .save()

    # Stop the Spark session
    # spark.stop()

    print("Transformation and data insertion completed.")


process_data_in_pyspark()
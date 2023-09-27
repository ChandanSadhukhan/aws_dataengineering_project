import datetime
import os
import mysql

from resource.dev.config import *
from src.main.utility import logging_config, spark_session, py_sql_session


def read_local_csvfile(directorypath: None):
    file_list = [f for f in os.listdir(directorypath)]
    csv_files = []

    if file_list:
        for file in file_list:
            if file.endswith('.csv'):
                csv_files.append(file)
        logging_config.logger.info(f"List of file '{file_list}'")
        return csv_files
    else:
        logging_config.logger.error(f"No file found")


def check_correct_schema_and_move(file_list: None):
    spark = spark_session.get_session()

    sql_config = py_sql_session.config

    current_date = datetime.datetime.today()
    formated_date = current_date.strftime("%Y-%m-%dT%H:%M")


    for file in file_list:
        file_path = sales_team_data_mart_partitioned_local_file + file
        data_schema = spark.read.format("csv") \
            .option("header", "true") \
            .load(file_path).columns

        logging_config.logger.info(f"Schema loaded for file: {file}")
        logging_config.logger.info(f"For file: '{file}' schecma is '{data_schema}'")

        missing_column = set(data_schema) - set(mandatory_columns)
        destination_folder = None

        statement = f"INSERT INTO dataengineering_project.product_staging_table (file_name, file_location, created_date, updated_date , status) VALUES('{file}', '{destination_folder}', '{formated_date}', '{formated_date}', 'M')"
        if missing_column:
            destination_folder = error_folder_path_local + file
            os.rename(file_path, destination_folder)
            logging_config.logger.info(f"Missing columns are : '{missing_column}'")
            logging_config.logger.info(f"Moved the file '{file}' in Error folder")


        else:
            logging_config.logger.info(f"There is no missing columns in the file {file}")

            connection = mysql.connector.connect(**sql_config)
            if connection.is_connected():
                logging_config.logger.info("Connected to MySQL database")
                cursor = connection.cursor()
                cursor.execute(statement)
                connection.commit()
                logging_config.logger.info("Entries are made in MySQL database")
                connection.close()




# Test
file_name = read_local_csvfile(sales_team_data_mart_partitioned_local_file)
check_correct_schema_and_move(file_name)

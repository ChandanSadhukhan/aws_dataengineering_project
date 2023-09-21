import logging
import os
from resource.dev.config import *
from src.main.utility import logging_config


def read_loacl_csvfile(directorypath: None):
    file_list = [f for f in os.listdir(directorypath)]

    if file_list:
        logging_config.logger.info(f"List of file '{file_list}'")
        return file_list
    else:
        logging.error(f"No file found")




# Test
read_loacl_csvfile(sales_team_data_mart_partitioned_local_file)
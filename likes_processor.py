import pandas as pd
import logging
from data_ingestion import read_json_file

class LikesProcessor:
    def __init__(self, config_params, logging_level='INFO'):
        """
        Initializes the LikesProcessor with the path to the JSON file and configuration parameters.
        
        :param config_params: Dictionary containing configuration parameters
        """
        self.file_path = config_params['likes_file_path']
        self.columns = config_params['likes_columns']
        self.columns_to_rename = config_params['likes_columns_to_rename']

        self.initialize_logging(logging_level)

        self.df = None

    def initialize_logging(self, logging_level):
        """
        Initializes the logging configuration.
        
        :param logging_level: Logging level to set (e.g., 'INFO', 'DEBUG', 'ERROR')
        """
        logger_name = __name__ + ".LikesProcessor"
        self.logger = logging.getLogger(logger_name)
        self.logger.propagate = False

        if logging_level.upper() == 'DEBUG':
            self.logger.setLevel(logging.DEBUG)
        elif logging_level.upper() == 'INFO':
            self.logger.setLevel(logging.INFO)
        elif logging_level.upper() == 'WARNING':
            self.logger.setLevel(logging.WARNING)
        elif logging_level.upper() == 'NONE':
            self.logger.disabled = True
        else:
            self.logger.setLevel(logging.INFO)
            self.logger.warning(f"Invalid logging level '{logging_level}'. Defaulting to INFO.")

        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
    
    def load_data(self):
        """
        Loads the likes data from the JSON file into a pandas DataFrame.
        """
        self.logger.info(f"Loading likes data from {self.file_path}")
        self.df = read_json_file(self.file_path)
    
    def select_columns(self):
        """
        Selects specific columns from the DataFrame based on the configuration parameters.
        """
        if self.df is not None:
            self.logger.info(f"Selecting columns: {self.columns}")
            self.df = self.df[self.columns]
        else:
            self.logger.error("DataFrame is empty. Cannot select columns.")

    def rename_columns(self):
        """
        Renames columns in the DataFrame based on the configuration parameters.
        """
        if self.df is not None:
            self.logger.info(f"Renaming columns: {self.columns_to_rename}")
            self.df.rename(columns=self.columns_to_rename, inplace=True)
        else:
            self.logger.error("DataFrame is empty. Cannot rename columns.")
        self.logger.info("Columns renamed successfully.")

    def format_dates(self):
        """
        Converts 'createdAt column to datetime format.
        """
        if self.df is not None:
            self.logger.info("Converting 'createdAtcolumn to datetime format.")
            self.df['createdAt'] = pd.to_datetime(self.df['createdAt'], unit='ms', errors='coerce')
            self.df['createdAt'] = self.df['createdAt'].dt.strftime('%Y-%m-%d %H:%M:%S')
        else:
            self.logger.error("Cannot convert datetime columns. DataFrame is None.")
    
    def process_likes(self):
        """
        Processes the likes data by loading, selecting columns, renaming columns, and formatting dates.
        """
        self.load_data()
        self.select_columns()
        self.rename_columns()
        self.format_dates()
        self.logger.info("Likes data processing completed successfully.")
#         self.logger.info("Users data processing completed successfully.")
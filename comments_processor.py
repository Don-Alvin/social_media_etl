import pandas as pd
import logging
from data_ingestion import read_json_file

class CommentsProcessor:
    def __init__(self, config_params, logging_level='INFO'):
        """
        Initializes the CommentsProcessor with the path to the JSON file and configuration parameters.
        
        :param config_params: Dictionary containing configuration parameters
        """
        self.file_path = config_params['comments_file_path']
        self.columns = config_params['comments_columns']
        self.columns_to_rename = config_params['comments_columns_to_rename']

        self.initialize_logging(logging_level)

        self.df = None

    def initialize_logging(self, logging_level):
        """
        Initializes the logging configuration.
        
        :param logging_level: Logging level to set (e.g., 'INFO', 'DEBUG', 'ERROR')
        """
        logger_name = __name__ + ".CommentsProcessor"
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
        Loads the comments data from the JSON file into a pandas DataFrame.
        """
        self.logger.info(f"Loading comments data from {self.file_path}")
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
    
    def process_comments(self):
        """
        Processes the comments data by loading, selecting, and renaming columns.
        """
        self.load_data()
        self.select_columns()
        self.rename_columns()
        self.logger.info("Comments data processing completed.")
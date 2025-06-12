from data_ingestion import read_json_file
import pandas as pd
import logging

class PostsProcessor:
    def __init__(self, config_params, logging_level='INFO'):
        """
        Initializes the PostsProcessor with the path to the JSON file.
        
        :param file_path: Path to the JSON file containing posts data
        """
        self.file_path = config_params['posts_file_path']
        self.columns = config_params['posts_columns']
        self.columns_to_rename = config_params['posts_columns_to_rename']

        self.initialize_logging(logging_level)

        self.df = None

    def initialize_logging(self, logging_level):
        """
        Initializes the logging configuration.
        
        :param logging_level: Logging level to set (e.g., 'INFO', 'DEBUG', 'ERROR')
            """
        logger_name = __name__ + ".PostsProcessor"
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

    # Load the posts data from the JSON file into a pandas DataFrame  
    def load_data(self):
        """
        Loads the posts data from the JSON file into a pandas DataFrame.
            """
        self.logger.info(f"Loading posts data from {self.file_path}")
        self.df = read_json_file(self.file_path)

        if self.df is not None:
            self.logger.info(f"Posts data loaded successfully with {len(self.df)} records.")
        else:
            self.logger.error("Failed to load posts data. DataFrame is None.")

    # Select only the specified columns
    def select_columns(self):
        """
        Selects only the specified columns from the DataFrame.
        """
        if self.df is not None:
            self.logger.info(f"Selecting columns: {self.columns}")
            self.df = self.df[self.columns]
            self.logger.info(f"Columns selected successfully. DataFrame now has {len(self.df)} records.")
        else:
            self.logger.error("Cannot select columns. DataFrame is None.")
        
    # Renames the columns of the DataFrame based on the provided mapping
    def rename_columns(self):
        """
        Renames the columns of the DataFrame based on the provided mapping.
        """
        if self.df is not None:
            self.logger.info("Renaming columns in posts DataFrame.")
            self.df.rename(columns=self.columns_to_rename, inplace=True)
            self.logger.info("Columns renamed successfully.")
        else:
            self.logger.error("Cannot rename columns. DataFrame is None.")
        
    # Change createdAt columns to datetime format
    def convert_datetime_columns(self):
        """
        Converts 'created_at' and 'updated_at' columns to datetime format.
        """
        if self.df is not None:
            self.logger.info("Converting 'created_at' and 'updated_at' columns to datetime format.")
            self.df['dateCreated'] = pd.to_datetime(self.df['dateCreated'], unit='ms', errors='coerce')
            self.df['dateCreated'] = self.df['dateCreated'].dt.strftime('%Y-%m-%d %H:%M:%S')
        else:
            self.logger.error("Cannot convert datetime columns. DataFrame is None.")
        
    def process_posts(self):
        """
        Processes the posts data by loading, renaming columns, and converting datetime columns.
        """
        self.load_data()
        self.select_columns()
        self.rename_columns()
        self.convert_datetime_columns()
        self.logger.info("Posts processing completed.")
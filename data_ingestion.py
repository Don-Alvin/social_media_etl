import json
from pandas import json_normalize
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Import data from a JSON file and convert it to a pandas DataFrame
def read_json_file(file_path):
    """
    Reads a JSON file and returns a pandas DataFrame.
    
    :param file_path: Path to the JSON file
    :return: DataFrame containing the JSON data
    """
    try:
        with open(file_path, 'r') as file:
            data = json.load(file)

        logging.info(f"Successfully read JSON file: {file_path}")
        df = json_normalize(data)
        logging.info(f"DataFrame created with {len(df)} records.")
        return df
    except ValueError as e:
        logging.error(f"ValueError: {e} - The file {file_path} is not a valid JSON file.")
        return None
    except FileNotFoundError as e:
        logging.error(f"FileNotFoundError: {e} - The file {file_path} does not exist.")
        return None
    except json.JSONDecodeError as e:
        logging.error(f"JSONDecodeError: {e} - The file {file_path} could not be decoded as JSON.")
        return None
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        return None
import json
import pandas as pd
import os

from constants import DEFAULT_CONTRACT_PATH

def load_json_data_list(file_path: str) -> list:
    """
    Loads data from a JSON file into a list of dictionaries.

    Args:
        file_path (str): The path to the JSON file.

    Returns:
        list: The loaded data as a list of dictionaries. Returns an empty list if the file is missing, empty, or invalid.
    """
    if not os.path.exists(file_path):
        print(f"Error: The file {file_path} was not found.")
        return []

    try:
        with open(file_path, "r") as f:
            data = json.load(f)

            if not data:
                print(f"Warning: The file {file_path} contains no data.")
                return []
            return data
        
    except Exception as e:
        print(f"Error: Failed to parse JSON file {file_path}. {e}")
        return []

def load_csv_data_df(file_path: str) -> pd.DataFrame:
    """
    Loads data from a CSV file into a pandas DataFrame.

    Args:
        file_path (str): The path to the CSV file.

    Returns:
        pd.DataFrame: The loaded data. Returns empty DataFrame if file is missing or empty.
    """
    if not os.path.exists(file_path):
        print(f"Error: The file {file_path} was not found.")
        return pd.DataFrame()
    
    try:
        df = pd.read_csv(file_path)
        if df.empty:
            print(f"Warning: The CSV file {file_path} contains no data.")
        return df

    except Exception as e:
        print(f"Error: Failed to read CSV file {file_path}. {e}")
        return pd.DataFrame()

def load_json_data_df(file_path: str) -> pd.DataFrame:
    """
    Loads data from a JSON file into a pandas DataFrame.

    Args:
        file_path (str): The path to the JSON file..

    Returns:
        pd.DataFrame: The loaded data. Returns empty DataFrame if file is missing or empty.
    """
    if not os.path.exists(file_path):
        print(f"Error: The file {file_path} was not found.")
        return pd.DataFrame()
    
    try:
        df = pd.read_json(file_path)
        if df.empty:
            print(f"Warning: The CSV file {file_path} contains no data.")
        return df
    
    except Exception as e:
        print(f"Error: Failed to read CSV file {file_path}. {e}")
        return pd.DataFrame()


def get_provider_to_contracts_dict(contract_path: str = DEFAULT_CONTRACT_PATH) -> dict:
    """
    Loads contracts for each provider as a dict

    Args:
        contract_path (str): Path to cleaned contracts JSON.

    Returns:
        pd.DataFrame: The dict contains provider as key where values contains their respective contracts.
            -e.g. 'Citywide Service Solutions Pty Ltd': [
                {'contract_title': 'Civil infrastructure services', 'contract_number': 3665}, 
                {'contract_title': 'Open spaces management - region 1', 'contract_number': 3676} , 
                {'contract_title': 'Tree management maintenance services', 'contract_number': 3678}
            ]
    """
    contract_df = load_json_data_df(contract_path)

    return contract_df.groupby("service_provider")[["contract_title", "contract_number"]].apply(lambda g: g.to_dict("records")).to_dict()
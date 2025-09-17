"""
ETL for Procurement Data

This module processes the messy procurement transaction dataset generated
by `data_generator.py` and transforms it into a clean, analysis-ready format.

Key ETL tasks:
    - Extract: Load messy CSV containing procurement transactions.

    - Normalize provider names (casing, remove dots/spaces, expand abbreviations).
    - Standardize invoice amounts to numeric floats in AUD.
    - Parse multiple inconsistent date formats into ISO format (YYYY-MM-DD).
    - Clean contract titles (strip whitespace, consistent casing).
    - Handle missing or blank contract numbers.
    - Handle mismatch service type based on provider name.
    - Track fields that failed cleaning (NeedsAttention).
    - Track fields that were modified/normalized (ModifiedFields).

    - Load: Save cleaned dataset into a structured CSV for analytics and dashboards.
"""

import pandas as pd
import numpy as np
import re
from datetime import datetime

from utils import *

from constants import *

from fuzzywuzzy import process, fuzz

from typing import Optional, Tuple

import os
from glob import glob

def clean_provider(provider: str, correct_providers: list, high_threshold: int = 80, low_threshold: int = 60) ->  Tuple[Optional[bool], Optional[str]]:
    """
    Clean and normalize provider names using a two-pass fuzzy matching strategy.

    Args:
        provider (str): Raw provider string with potential inconsistencies.
        correct_providers (list): List of known, correct provider names.
        high_threshold (int): High similarity threshold for first-pass strict matching.
        low_threshold (int): Lower similarity threshold for second-pass lenient matching.

    Returns:
        Tuple[Optional[bool], Optional[str]]: (was_modified, cleaned_provider) or (None, original_input) if failed
        
    """
    if not provider or not isinstance(provider, str):
        return (None, provider)

    original = provider.strip()

    # --- First pass: strict matching ---
    best_match = process.extractOne(original, correct_providers, scorer=fuzz.token_set_ratio)

    if best_match and best_match[1] >= high_threshold:
        if best_match[0] != original:
            return (True, best_match[0])
        else:
            return (False, original)

    # --- Second pass: normalize and fuzzy match ---
    # Preliminary cleaning
    # - Strip leading/trailing spaces
    # - Insert spaces before uppercase letters to handle run-on words (camel case) (e.g. "VictorianYMCA" -> "Victorian YMCA")
    # - Remove common suffix variations like "(AU)" or "(AUS)" to normalize
    cleaned = re.sub(r'(?<=[a-z])([A-Z])', r' \1', original).strip()
    cleaned = re.sub(r"\(AU\)|(AUS)", "", cleaned).strip()

    # Perform fuzzy matching against the list of correct provider names
    # - `extractOne` compares `cleaned` provider name to all known providers
    # - Returns the best match as a tuple: (matched_string, similarity_score)
    best_match = process.extractOne(cleaned, correct_providers, scorer=fuzz.token_set_ratio)

    # Validate the fuzzy match by checking the similarity score
    # - If the score exceeds the threshold, accept the matched provider name
    # - Otherwise, keep the original (possibly messy) provider name
    if best_match and best_match[1] > low_threshold:

        if best_match[0] != original:
            return (True, best_match[0])
        else:
            return (False, original)
    else:
        # Fallback: no strong match found, return the original 
        return (None, original)


def clean_invoice_amount(amount: str) -> Tuple[Optional[bool], Optional[float]]:
    """
    Cleans and converts a possible messy invoice amount string into a float.
    
    This function handles various formats, including currency symbols,
    shorthand for millions, and missing values.
    
    Args:
        amount (str): The possible messy invoice amount string.
        
    Returns:
        Tuple[Optional[bool], Optional[float]]: (was_modified, cleaned_amount) or (None, original_input) if failed
    """

    # Handle missing or invalid inputs
    if amount is None or (isinstance(amount, float) and np.isnan(amount)):
        return (None, amount)
    if not isinstance(amount, str):
        amount = str(amount)

    
    original = amount.strip()

    # Standardize to lowercase and remove common non-essential characters
    amount_str = original.lower().strip()
    
    # Handle millions shorthand (e.g., 1.2m, 1.2 mil, 1.2 million)
    if "m" in amount_str:
        # Remove 'm', 'mil', 'million', and any surrounding whitespace
        amount_str = re.sub(r"m|mil|million", "", amount_str).strip()
        # Remove all other non-numeric characters except the decimal point
        cleaned_amount_str = re.sub(r"[^\d.]", "", amount_str)
        try:
            # Convert to float and multiply by 1,000,000
            return (True, float(cleaned_amount_str) * DEFAULT_UNIT)
        except ValueError:
            # Fallback: Fail to convert to float,return the original 
            return (None, original)
    
    # Clean the string for numeric conversion
    # This regex now correctly removes all non-digit and non-dot characters
    cleaned_amount_str = re.sub(r"[^\d.]", "", amount_str)

    # Handle cases with multiple decimal points
    if cleaned_amount_str.count('.') > 1:
        parts = cleaned_amount_str.split('.')
        cleaned_amount_str = ''.join(parts[:-1]) + '.' + parts[-1]
    
    # Final conversion to float
    try:
        value = float(cleaned_amount_str)
        
        # Determine if the value was modified
        was_modified = cleaned_amount_str != original
        
        return (was_modified, value)
    except (ValueError, TypeError):
        # Fallback: Fail to convert to float,return the original 
        return (None, original)
    


def clean_date(date_str: str) -> Tuple[Optional[bool], Optional[str]]:
    """
    Cleans and standardizes a date string to a consistent 'YYYY-MM-DD' format.
    
    This version includes more common formats for better generalization.
    
    Args:
        date_str (str): The possibly messy date string.
        
    Returns:
        Tuple[Optional[bool], Optional[str]]: (was_modified, cleaned_date) or (None, original_input) if failed
    """
    # Handle missing or invalid inputs
    if pd.isna(date_str) or not isinstance(date_str, str) or not date_str.strip():
        return (None, date_str)
    
    original = date_str.strip()
    
    # Normalize separators to improve parsing consistency
    # Replace spaces, slashes, commas with dashes
    date_str_clean = (
        date_str.strip()
        .replace(",", "")
        .replace("/", "-")
        .replace(" ", "-")
    )

    # Attempt parsing against common procurement date formats
    formats = [
        "%Y-%m-%d",      # 2025-01-01 (ISO)
        "%d-%m-%Y",      # 01-01-2025 (AU/EU)
        "%m-%d-%Y",      # 01-25-2025 (US)
        "%d-%b-%Y",      # 01-Jan-2025
        "%d-%B-%Y",      # 01-January-2025
        "%B-%d-%Y",      # January-01-2025
        "%y-%m-%d"       # 25-01-01 (2-digit year)
    ]

    for fmt in formats:
        try:
            cleaned = datetime.strptime(date_str_clean, fmt).strftime("%Y-%m-%d")
            if cleaned != original:
                return (True, cleaned)
            else:
                return (False, original)
        except ValueError:
            continue

    return (None, original)



def clean_title(provider_clean: Optional[str], contract_title: Optional[str], contract_num: Optional[str], provider_to_contracts_dict: dict) -> Tuple[Optional[bool], Optional[str]]:
    """
    Cleans and corrects ContractTitle for a single transaction row based on the provider name.

    Args:
        provider_clean (str or None): Cleaned provider name from Provider_Clean column.
        contract_title (str or None): Original ContractTitle value (may be empty or incorrect).
        contract_num (str or None): Original ContractNumber value (may be empty or incorrect).
        provider_to_contracts_dict (dict): Mapping of provider -> list of contract dicts
                                           with keys 'contract_title' and 'contract_number'.

    Returns:
        Tuple[Optional[bool], Optional[str]]: (was_modified, cleaned_title) or (None, original_input) if failed
    """
    # If title is empty and provider is missing or not in mapping, cannot fix anything
    if (pd.isna(contract_title) or str(contract_title).strip() == "") and (provider_clean not in provider_to_contracts_dict):
        return (None, contract_title)

    contracts = provider_to_contracts_dict.get(provider_clean)
    # When provider_clean is invalid, cant verify the given title
    if not contracts:
        return (None, contract_title)
    
    valid_titles = {contract["contract_title"] for contract in contracts}

    #  Missing or mismatched title -> replace with contract with same contract number
    if contract_title not in valid_titles:

        # Contract number is missing, unable to match potential contract titke
        if pd.isna(contract_num) or str(contract_num).strip() == "":
            return (None, contract_title)
        
        # Apply if statement to improve efficiency for most of the cases
        match_contract_len = len(contracts)
        if match_contract_len == 1:
            return (True, contracts[0]['contract_title'])
        
        if match_contract_len > 1:
            for contract in contracts:
                if str(contract['contract_number']) == contract_num.strip():
                    return (True, contract['contract_title'])
        
        # Fallback: nothing match
        return (None, contract_title)

    return (False, contract_title)


def clean_number(provider_clean: Optional[str], contract_number: Optional[str], title_clean: Optional[str], provider_to_contracts_dict: dict) -> Tuple[Optional[bool], Optional[str]]:
    """
    Cleans and fills missing ContractNumber for a single transaction row.

    Args:
        provider_clean (str or None): Cleaned provider name from Provider_Clean column.
        contract_number (str or None): Original ContractNumber value (may be empty).
        title_clean (str or None): Cleaned contract title from ContractTitle_Clean column.
        provider_to_contracts_dict (dict): Mapping of provider -> list of contract dicts
                                           with keys 'contract_title' and 'contract_number'.

    Returns:
        Tuple[Optional[bool], Optional[str]]: (was_modified, cleaned_title) or (None, original_input) if failed
    """
    # If number is empty and provider is missing or not in mapping, cannot fix anything
    if (pd.isna(contract_number) or str(contract_number).strip() == "") and (provider_clean not in provider_to_contracts_dict):
        return (None, contract_number)
    
    contracts = provider_to_contracts_dict.get("")
    # When provider_clean is invalid, cant verify the given number
    if not contracts:
        return (None, contract_number)

    valid_nums = {contract["contract_number"] for contract in contracts}
    
    #  Missing or mismatched title -> replace with contract with same contract number
    if contract_number not in valid_nums:

        # Contract title is missing, unable to match potential contract number
        if pd.isna(title_clean) or str(title_clean).strip() == "":
            return (None, contract_number)
        
        #  Missing or mismatched title -> replace with contract with same contract title
        match_contract_len = len(contracts)
        if match_contract_len == 1:
            return (True, contracts[0]['contract_number'])
        
        if match_contract_len > 1:
            for contract in contracts:
                if str(contract['contract_title']) == title_clean:
                    return (True, contract['contract_number'])

        # Fallback: nothing match
        return (None, contract_number)

    return (False, contract_number)



# ---------------------------------------------------------------------------
# ETL Helper Functions
# ---------------------------------------------------------------------------

def record_issue(
    df: pd.DataFrame, 
    failed_col: str = "FailedFields", 
    modified_col: str = "ModifiedFields"
) -> pd.DataFrame:
    """
    Scans *_Flag columns in the DataFrame and updates two list columns:
    
    1. `FailedFields` records fields where automatic cleaning failed (value is None/NaN) 
       and requires manual attention.
    2. `ModifiedFields` records fields where automatic cleaning succeeded (value is True) 
       and the system was able to fix the value.

    Args:
        df (pd.DataFrame): The input DataFrame containing *_Flag columns.
        failed_col (str, optional): Name of the column to store fields that failed cleaning.
                                    Defaults to 'FailedFields'.
        modified_col (str, optional): Name of the column to store fields that were modified automatically.
                                      Defaults to 'ModifiedFields'.

    Returns:
        pd.DataFrame: The DataFrame with updated FailedFields and ModifiedFields columns.
    """

    # Initialize the failed_col as a list column, ensuring all rows are lists
    if failed_col not in df.columns:
        df[failed_col] = [[] for _ in range(len(df))]
    else:
        # Convert existing values to lists if they are not already
        df[failed_col] = df[failed_col].apply(lambda x: x if isinstance(x, list) else [])

    # Initialize the modified_col as a list column, ensuring all rows are lists
    if modified_col not in df.columns:
        df[modified_col] = [[] for _ in range(len(df))]
    else:
        # Convert existing values to lists if they are not already
        df[modified_col] = df[modified_col].apply(lambda x: x if isinstance(x, list) else [])

    # Find all columns ending with "_Flag" to determine which fields to process
    flag_cols = [c for c in df.columns if c.endswith("_Flag")]
    for col in flag_cols:
        # Derive the original field name by removing the "_Flag" suffix
        field_name = col.replace("_Flag", "")

        # Identify rows where the flag is None -> cleaning failed
        failed_mask = df[col].isna()
        # Append the field name to the failed_col list for these rows
        df.loc[failed_mask, failed_col] = df.loc[failed_mask, failed_col].apply(
            lambda issue_list: issue_list + [field_name]
        )

        # Identify rows where the flag is True -> field was successfully modified automatically
        modified_mask = df[col] == True
        # Append the field name to the modified_col list for these rows
        df.loc[modified_mask, modified_col] = df.loc[modified_mask, modified_col].apply(
            lambda issue_list: issue_list + [field_name]
        )

    return df

def etl_pipeline(
    invoice_path: str,
    contract_path: str = DEFAULT_CONTRACT_PATH,
    master_cleaned_path: str = DEFAULT_CLEANED_PATH
) -> pd.DataFrame:
    """
    ETL Pipeline for Procurement Transactions.

    This function executes the Extract-Transform-Load workflow on a single raw
    procurement transactions CSV file and appends the cleaned results into a
    rolling master dataset.

    Args:
        invoice_path (str):
            Path to raw transactions CSV file (simulated_transactions_*.csv).
        contract_path (str):
            Path to contract dataset JSON (default: DEFAULT_CONTRACT_PATH).
        master_cleaned_path (str):
            File path for rolling cleaned dataset. If file exists, new data
            will be appended; otherwise, file will be created.

    Returns:
        pd.DataFrame:
            DataFrame containing the cleaned subset of the most recent batch.
    """


    print(f"Running ETL for {invoice_path}...")

    # --- Step 1: Load data ---
    invoice_df = load_csv_data_df(invoice_path)
    provider_to_contracts_dict = get_provider_to_contracts_dict(contract_path)

    # --- Step 2: Clean core columns ---
    invoice_df[["Provider_Flag", "Provider_Clean"]] = invoice_df["Provider"].apply(
        lambda x: pd.Series(clean_provider(x, list(provider_to_contracts_dict.keys())))
    )

    invoice_df[["InvoiceAmount_Flag", "InvoiceAmount_Clean"]] = invoice_df["InvoiceAmount"].apply(
        lambda x: pd.Series(clean_invoice_amount(x))
    )

    invoice_df[["InvoiceDate_Flag", "InvoiceDate_Clean"]] = invoice_df["InvoiceDate"].apply(
        lambda x: pd.Series(clean_date(x))
    )

    invoice_df[["ContractTitle_Flag", "ContractTitle_Clean"]] = invoice_df.apply(
        lambda row: pd.Series(clean_title(
            row["Provider_Clean"],
            row["ContractTitle"],
            row["ContractNumber"],
            provider_to_contracts_dict
        )),
        axis=1
    )

    invoice_df[["ContractNumber_Flag", "ContractNumber_Clean"]] = invoice_df.apply(
        lambda row: pd.Series(clean_number(
            row["Provider_Clean"],
            row["ContractNumber"],
            row["ContractTitle_Clean"],
            provider_to_contracts_dict
        )),
        axis=1
    )

    # --- Step 3: Track issues ---
    invoice_df = record_issue(invoice_df)

    # --- Step 4: Append cleaned data ---
    cols_to_keep = ["InvoiceID", "FailedFields", "ModifiedFields"] + \
                   [c for c in invoice_df.columns if c.endswith("_Clean")]

    cleaned_subset = invoice_df[cols_to_keep]

    if os.path.exists(master_cleaned_path):
        cleaned_subset.to_csv(master_cleaned_path, mode="a", header=False, index=False)
        print(f"Appended cleaned data to {master_cleaned_path}")
    else:
        cleaned_subset.to_csv(master_cleaned_path, mode="w", header=True, index=False)
        print(f"Created {master_cleaned_path} with first batch of cleaned data")

    return cleaned_subset


if __name__ == "__main__":


    data_dir = os.path.join(os.path.dirname(__file__), "..", "data")
    pattern = os.path.join(data_dir, "simulated_transactions*.csv")
    files = sorted(glob(pattern))

    if not files:
        print(f"No simulated transaction files found in {data_dir}. Please run data_generator.py first.")
    else:
        latest_file = files[-1]
        print(f"Running ETL on latest file: {latest_file}")
        etl_pipeline(latest_file)

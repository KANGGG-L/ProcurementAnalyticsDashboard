"""
Procurement Data Generator

This module generates simulated procurement transaction data based on scraped government contracts.
The purpose is to mimic the structured and messy real-world data would encounter in procurement systems

The generator introduces:
    - Variations in provider names (typos, casing, abbreviations).
    - Inconsistent invoice amount formats (currency symbols, shorthand, missing values).
    - Multiple date formats and occasional missing dates.
    - Mismatch service type.
    - Annually accumulated invoices amount that may not align perfectly with annual contract values
      (some higher, some lower) to simulate real-world financial behaviour.
    - Missed contract number.
    

The dataset can be used to test ETL pipelines, data cleaning scripts,
and procurement analytics dashboards.
"""

import pandas as pd
import random
from datetime import datetime, timedelta
from utils import load_json_data_list
from constants import *
from datetime import datetime, timedelta

import os
from glob import glob

def generate_provider(provider: str) -> str:
    """
    Create realistic variations of provider names to simulate data inconsistencies.

    Variations include:
        - Casing differences (UPPERCASE, lowercase, title case).
        - Added suffixes (e.g., "(AU)", "(AUS)").
        - Truncated or shortened names (first-half section and first word).
        - Common abbreviations (e.g., Management -> Mgmt, International -> Intl).
        - Extra dots or missing dots
        - Random typos and misspellings.
        - Extra or missing spaces.

    Args:
        provider (str): The provider's official/legal name.

    Returns:
        str: A randomly chosen variation of the provider's name.
    """
    # Basic variations
    options = [
        provider,                          # original name
        provider.upper(),                  # all uppercase
        provider.lower(),                  # all lowercase
        provider.title(),                  # title case
        provider.strip(),                  # remove leading/trailing whitespace
        provider + " (AU)",                # add suffix
        provider + " (AUS)",               # add alternative suffix
        provider.split()[0],               # first word only
        provider[:max(5, len(provider)//2)]# truncate halfway (e.g., "Cleanaway Waste M")
    ]

    # Common substitutions/abbreviations to simulate messy data
    substitutions = {
        "Limited": ["Ltd", "LTD"],
        "Pty Ltd": ["Pty. Ltd.", "P/L"],
        "Management": ["Mgmt"],
        "International": ["Intl", "Int’l"],
        ".": ["..", "", ",", "/"]
    }

    # Apply substitutions and append to variations
    for key, subs in substitutions.items():
        if key in provider:
            for s in subs:
                options.append(provider.replace(key, s))

    # Introduce a random typo
    if len(provider) > 4:
        typo_index = random.randint(0, len(provider) - 1)
        typo_char = random.choice("abcdefghijklmnopqrstuvwxyz")
        typo_version = provider[:typo_index] + typo_char + provider[typo_index + 1:]
        options.append(typo_version)

    # Modify spaces for more messiness
    if " " in provider:
        options.append(provider.replace(" ", "  "))  # double spaces
        options.append(provider.replace(" ", ""))    # remove all spaces

    return random.choice(options)


def generate_date(start_year: int = 2025, end_year: int = 2030, miss_possibility: float = DEFAULT_POSSIBILITY) -> str:
    """
    Generate a random date within a range, with inconsistent formatting.
    Simulates messy invoice or contract dates in procurement data.

    Args:
        start_year (int, optional): Start year for date generation (default: 2025)
        end_year (int, optional): End year for date generation (default: 2030)
        miss_possibility (float, optional): Possibility of leave date as empty

    Returns:
        str: A randomly formatted date, or empty string (5% chance).
    """
    # Define date range
    start = datetime(start_year, 1, 1)
    end = datetime(end_year, 1, 1)

    # Generate a random date within the range
    date_obj = start + timedelta(days=random.randint(0, (end - start).days))

    # Various formats to mimic inconsistent data
    options = [
        "%Y-%m-%d",   # 2025-01-01 (ISO)
        "%d/%m/%Y",   # 01/01/2025 (AU style)
        "%d-%b-%Y",   # 01-Jan-2025
        "%d %B %Y",   # 01 January 2025
        "%y/%m/%d",   # 25/01/01
        "" if random.random() < miss_possibility else "%d/%m/%Y"  # occasional missing
    ]
    
    return date_obj.strftime(random.choice(options))


def generate_invoice_amount(unit: int = DEFAULT_UNIT, miss_possibility: float = DEFAULT_POSSIBILITY) -> str:
    """
    Generate a messy invoice amount simulating real-world procurement data.

    Variations include:
        - Plain numbers
        - Currency symbols ($)
        - Currency suffix (AUD)
        - Shorthand (millions: 1.2m)
        - Occasionally missing values

     Args:
        unit (int, optional): Base unit multiplier. Defaults to 1,000,000.
        miss_possibility (float, optional): Possibility of leave amount as empty

    Returns:
        str: A randomly formatted invoice amount, or empty string (5% chance).
    """
    # Generate a base amount (simulate realistic invoice amounts)
    amount = round(random.uniform(200, 250_000), 2)

    # Possible messy formats
    options = [
        f"${amount:,}",               # e.g., $12,345.67
        f"{amount} AUD",              # with currency suffix
        f"{amount/unit:.1f}m",        # shorthand for millions
        str(amount),                  # plain number
        "" if random.random() < miss_possibility else str(amount)  # occasional missing
    ]

    return random.choice(options)


def generate_transactions(contracts: list, num_records: int = 1000, miss_possibility: float = DEFAULT_POSSIBILITY, start_invoice_id: int = 10000) -> pd.DataFrame:
    """
    Generate a synthetic procurement transactions dataset with realistic mistakes.

    Each transaction is based on a random contract and includes variations
    in provider names, invoice amounts, dates, and occasional missing values.

    Args:
        contracts (list): List of contract records loaded from `contracts.json`.
                        Each record is a dictionary with keys such as:
                        - "service_provider": Name of the supplier/company.
                        - "contract_title": Title of the contract.
                        - "contract_number": Unique identifier of the contract.
        num_records (int, optional): Number of transaction records to generate.
        miss_possibility (float, optional): Possibility of randomly assign ContractTitle AND leave ContractNumber as empty
        start_invoice_id (float, optional): The invocie id that will be incremental at the transactions

    Returns:
        pd.DataFrame: A DataFrame containing messy procurement transaction records.
    """

    records = []

    title_options = list({c["contract_title"] for c in contracts})

    for i in range(num_records):
        # Pick a random contract as the seed for this transaction
        contract = random.choice(contracts)

        # Create the messy transaction record
        records.append({
            "InvoiceID": f"INV{start_invoice_id+i}",
            "ContractTitle": random.choice(title_options) if random.random() < miss_possibility else contract.get("contract_title", ""),
            "Provider": generate_provider(contract.get("service_provider", "")),
            "InvoiceAmount": generate_invoice_amount(),
            "InvoiceDate": generate_date(2025, 2030),
            "ContractNumber": " " if random.random() < miss_possibility else contract.get("contract_number", "")
        })

    return pd.DataFrame(records)


if __name__ == "__main__":
    # Load contracts JSON created by the scraper
    contracts = load_json_data_list(DEFAULT_CONTRACT_PATH)

    # Ensure data folder exists
    data_dir = os.path.join(os.path.dirname(__file__), "..", "data")

    # Find last used InvoiceID if previous files exist
    files = sorted(glob(os.path.join(data_dir, "simulated_transactions_*.csv")))
    if files:
        latest_file = files[-1]
        df_existing = pd.read_csv(latest_file)
        if "InvoiceID" in df_existing.columns and not df_existing.empty:
            last_id = max(int(x.replace("INV", "")) for x in df_existing["InvoiceID"] if str(x).startswith("INV"))
        else:
            last_id = 10000
    else:
        last_id = 10000

    # Generate synthetic messy transactions starting after last InvoiceID
    df = generate_transactions(contracts, start_invoice_id=last_id + 1)

    # Save the simulated dataset with timestamp
    timestamp = datetime.now().strftime("%Y%m%d")
    output_file = os.path.join(data_dir, f"simulated_transactions_{timestamp}.csv")
    df.to_csv(output_file, index=False)

    print(f"Simulated data saved to {output_file}")

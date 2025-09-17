"""
Risk Scoring for Procurement Data

Consumes the cleaned procurement dataset from `etl.py`
and assigns a composite risk score to each transaction.

Risk factors considered:
    - Data quality issues
    - Contract compliance anomalies
    - Financial anomalies

Output:
    - risk_scored_invoices.csv (invoice-level risks)
"""

import pandas as pd
import numpy as np
from constants import DEFAULT_CLEANED_PATH, DEFAULT_CONTRACT_PATH, DEFAULT_RISK_PATH
from utils import load_csv_data_df, load_json_data_list

# --- Risk weights ---
FAILED_WEIGHTS = {
    "Provider": 5,
    "InvoiceAmount": 8,
    "InvoiceDate": 3,
    "Title": 5,
    "Number": 5,
}

MODIFIED_WEIGHTS = {
    "Provider": 2,
    "InvoiceAmount": 4,
    "InvoiceDate": 1,
    "Title": 2,
    "Number": 2,
}

CONTRACT_WEIGHTS = {
    "Expired": 15,
    "ExpiringSoon": 5,
    "ContractMismatch": 10,
}

FINANCIAL_WEIGHTS = {
    "HighAmount": 10,
    "LowAmount": 7,
}

HIGH_AMOUNT_THRESHOLD = 1000000
LOW_AMOUNT_THRESHOLD = 100


def safe_float(x):
    """Convert to float safely; return NaN if invalid."""
    try:
        return float(x)
    except (TypeError, ValueError):
        return np.nan


def safe_date(x):
    """Convert to datetime safely; return NaT if invalid."""
    try:
        return pd.to_datetime(x, errors="coerce")
    except Exception:
        return pd.NaT


def compute_risk_score(row: pd.Series, contract_map: dict) -> dict:
    """
    Compute detailed risk components and overall score for a single row.
    Handles invalid/missing inputs safely.
    """
    data_quality_risk = 0
    contract_risk = 0
    financial_risk = 0

    # --- Data Quality Risks ---
    failed_fields = row.get("FailedFields", []) or []
    modified_fields = row.get("ModifiedFields", []) or []

    for field in failed_fields:
        base = str(field).replace("_Clean", "").replace("_Flag", "").capitalize()
        data_quality_risk += FAILED_WEIGHTS.get(base, 5)

    for field in modified_fields:
        base = str(field).replace("_Clean", "").replace("_Flag", "").capitalize()
        data_quality_risk += MODIFIED_WEIGHTS.get(base, 2)

    # --- Extract key fields ---
    provider = row.get("Provider_Clean")
    title = row.get("ContractTitle_Clean")
    number = row.get("ContractNumber_Clean")

    amount = safe_float(row.get("InvoiceAmount_Clean"))
    inv_date = safe_date(row.get("InvoiceDate_Clean"))

    # --- Contract Compliance Risks ---
    key = (provider, title, number)
    contract = contract_map.get(key)

    if contract:
        expiry_raw = contract.get("expiry_date")
        expiry = safe_date(expiry_raw)

        if pd.notna(expiry) and pd.notna(inv_date):
            if inv_date > expiry:
                contract_risk += CONTRACT_WEIGHTS["Expired"]
            elif 0 <= (expiry - inv_date).days <= 90:
                contract_risk += CONTRACT_WEIGHTS["ExpiringSoon"]
    else:
        contract_risk += CONTRACT_WEIGHTS["ContractMismatch"]

    # --- Financial Risks ---
    if pd.notna(amount):
        if amount > HIGH_AMOUNT_THRESHOLD:
            financial_risk += FINANCIAL_WEIGHTS["HighAmount"]
        elif amount < LOW_AMOUNT_THRESHOLD:
            financial_risk += FINANCIAL_WEIGHTS["LowAmount"]

    total_score = data_quality_risk + contract_risk + financial_risk

    return {
        "RiskScore": total_score,
        "DataQualityRisk": data_quality_risk,
        "ContractRisk": contract_risk,
        "FinancialRisk": financial_risk,
    }


def add_risk_scores(df: pd.DataFrame, contracts: list) -> pd.DataFrame:
    """
    Apply risk scoring across dataset.
    """
    # Build contract lookup map
    contract_map = {
        (c.get("service_provider"), c.get("contract_title"), c.get("contract_number")): c
        for c in contracts
        if c.get("service_provider") and c.get("contract_title")
    }

    risk_dicts = df.apply(lambda row: compute_risk_score(row, contract_map), axis=1)
    risk_df = pd.DataFrame(list(risk_dicts))
    return pd.concat([df.reset_index(drop=True), risk_df], axis=1)


def main(cleaned_path: str = DEFAULT_CLEANED_PATH, contract_path: str = DEFAULT_CONTRACT_PATH, output_path: str = DEFAULT_RISK_PATH):
    """
    Run risk scoring pipeline end-to-end.
    """
    # Load cleaned transactions
    df = load_csv_data_df(cleaned_path)

    # Load contract seed
    contracts = load_json_data_list(contract_path)

    # Compute risk scores
    df = add_risk_scores(df, contracts)

    # Save enriched dataset
    df.to_csv(output_path, index=False)
    print(f"Risk-scored data saved to {output_path}")


if __name__ == "__main__":
    main()

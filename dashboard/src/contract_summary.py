"""
Contract Summary Module

This module consumes risk-scored invoices and computes supplier–contract spend summaries:
    1. Annual compliance (against contract lower/upper bounds).
    2. Monthly spend trends.
    3. Aggregated risk metrics per supplier–contract–period.

Outputs two files:
    - supplier_summary_annual.csv
    - supplier_summary_monthly.csv
"""

import pandas as pd
from utils import load_csv_data_df, load_json_data_list
from constants import (
    DEFAULT_RISK_PATH,
    DEFAULT_CONTRACT_PATH,
    DEFAULT_SUMMARY_ANNUAL_PATH,
    DEFAULT_SUMMARY_MONTHLY_PATH,
)


def compute_annual_summary(df: pd.DataFrame, contracts: list) -> pd.DataFrame:
    """
    Compute annual spend + risk aggregates per supplier–contract–year.
    """
    annual_summary = (
        df.groupby(["Provider_Clean", "ContractTitle_Clean", "ContractNumber_Clean", "Year"])
        .agg({
            "InvoiceAmount_Clean": "sum",
            "RiskScore": "mean",
            "DataQualityRisk": "mean",
            "ContractRisk": "mean",
            "FinancialRisk": "mean"
        })
        .reset_index()
        .rename(columns={"InvoiceAmount_Clean": "AnnualSpend"})
    )

    # Map for compliance check
    provider_contract_map = {
        (c["service_provider"], c["contract_title"], c["contract_number"]): c
        for c in contracts
    }

    def compliance(row):
        key = (row["Provider_Clean"], row["ContractTitle_Clean"], row["ContractNumber_Clean"])

        contract = provider_contract_map.get(key)
        if not contract:
            return "ContractMismatch"
        
        lower = contract.get("annual_value_lower_bound")
        upper = contract.get("annual_value_upper_bound")

        if upper and row["AnnualSpend"] > upper:
            return "OverUpper"
        elif lower and row["AnnualSpend"] < lower:
            return "UnderLower"
        return "WithinBounds"

    annual_summary["ComplianceFlag"] = annual_summary.apply(compliance, axis=1)
    return annual_summary


def compute_monthly_summary(df: pd.DataFrame, contracts: list) -> pd.DataFrame:
    """
    Compute monthly spend + risk aggregates per supplier–contract–year–month.
    """
    monthly_summary = (
        df.groupby(["Provider_Clean", "ContractTitle_Clean", "ContractNumber_Clean", "Year", "Month"])
        .agg({
            "InvoiceAmount_Clean": "sum",
            "RiskScore": "mean",
            "DataQualityRisk": "mean",
            "ContractRisk": "mean",
            "FinancialRisk": "mean"
        })
        .reset_index()
        .rename(columns={"InvoiceAmount_Clean": "MonthlySpend"})
    )

    # Add compliance per month (uses annual bounds for now)
    provider_contract_map = {
        (c["service_provider"], c["contract_title"], c["contract_number"]): c
        for c in contracts
    }

    def compliance(row):
        key = (row["Provider_Clean"], row["ContractTitle_Clean"], row["ContractNumber_Clean"])
        contract = provider_contract_map.get(key)
        if not contract:
            return "ContractMismatch"

        lower = contract.get("annual_value_lower_bound")
        upper = contract.get("annual_value_upper_bound")

        if upper and row["MonthlySpend"] > upper / 12:
            return "OverUpper"
        elif lower and row["MonthlySpend"] < lower / 12:
            return "UnderLower"
        return "WithinBounds"

    monthly_summary["ComplianceFlag"] = monthly_summary.apply(compliance, axis=1)
    return monthly_summary


def export_contract_summary(df: pd.DataFrame, contracts: list):
    """
    Export both annual and monthly supplier–contract summaries.
    """
    annual_summary = compute_annual_summary(df, contracts)
    monthly_summary = compute_monthly_summary(df, contracts)

    annual_summary.to_csv(DEFAULT_SUMMARY_ANNUAL_PATH, index=False)
    monthly_summary.to_csv(DEFAULT_SUMMARY_MONTHLY_PATH, index=False)

    print(f"Saved annual summary to {DEFAULT_SUMMARY_ANNUAL_PATH}")
    print(f"Saved monthly summary to {DEFAULT_SUMMARY_MONTHLY_PATH}")


def main(risk_path: str = DEFAULT_RISK_PATH,
         contract_path: str = DEFAULT_CONTRACT_PATH):
    """
    Run contract summary pipeline end-to-end.
    """
    df = load_csv_data_df(risk_path)

    df["Year"] = pd.to_datetime(df["InvoiceDate_Clean"], errors="coerce").dt.year
    df["Month"] = pd.to_datetime(df["InvoiceDate_Clean"], errors="coerce").dt.month

    

    contracts = load_json_data_list(contract_path)

    export_contract_summary(df, contracts)


if __name__ == "__main__":
    main()

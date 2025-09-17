"""
Export Power BI Module

This module consolidates outputs from ETL, risk scoring, contract summary,
and analysis into curated CSVs with stable schemas for Power BI dashboards.

Responsibilities:
    1. Load intermediate outputs (cleaned invoices, risk scores, summaries, forecasts, KPIs, scenarios).
    2. Validate key columns (e.g., Provider, ContractTitle, InvoiceAmount).
    3. Standardize column order and naming for BI consistency.
    4. Export curated CSVs for Power BI consumption.

Final Outputs:
    - invoices_curated.csv          (cleaned transaction-level data)
    - risk_scores_curated.csv       (transactions + risk scores)
    - supplier_summary_annual.csv   (annual contract compliance)
    - supplier_summary_monthly.csv  (monthly spend trends)
    - forecasting.csv               (monthly forecast)
    - annual_forecast.csv           (annual forecast)
    - kpis.csv                      (procurement KPIs)
    - scenario_model.csv            (scenario modeling results)
"""

import pandas as pd
import os
from constants import *
from utils import load_csv_data_df


# --- Utility functions ---
def validate_columns(df: pd.DataFrame, required: list, name: str) -> pd.DataFrame:
    """Ensure required columns exist, fill if missing."""
    for col in required:
        if col not in df.columns:
            print(f"[Warning] Missing column '{col}' in {name}, filling with blanks.")
            df[col] = None
    return df


def export_curated(df: pd.DataFrame, path: str, order: list | None = None):
    """Save curated CSV with column order enforced if provided."""
    if df.empty:
        print(f"[Warning] No data to export for {path}")
        return
    if order:
        existing_cols = [c for c in order if c in df.columns]
        df = df[existing_cols]
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path, index=False)
    print(f"Exported curated dataset: {path}")


# --- Main workflow ---
def main():
    # 1. Load raw outputs
    cleaned = load_csv_data_df(DEFAULT_CLEANED_PATH)
    risks = load_csv_data_df(DEFAULT_RISK_PATH)
    annual = load_csv_data_df(DEFAULT_SUMMARY_ANNUAL_PATH)
    monthly = load_csv_data_df(DEFAULT_SUMMARY_MONTHLY_PATH)
    monthly_forecast = load_csv_data_df(DEFAULT_ANALYSIS_MONTHLY_FORECAST_PATH)
    annual_forecast = load_csv_data_df(DEFAULT_ANALYSIS_ANNUAL_FORECAST_PATH)
    kpis = load_csv_data_df(DEFAULT_ANALYSIS_KPIS_PATH)
    scenario = load_csv_data_df(DEFAULT_ANALYSIS_SCENARIO_PATH)

    # 2. Validate essential columns
    cleaned = validate_columns(
        cleaned,
        ["Provider_Clean", "ContractTitle_Clean", "ContractNumber_Clean", "InvoiceDate_Clean", "InvoiceAmount_Clean"],
        "Cleaned Data",
    )

    risks = validate_columns(
        risks,
        ["Provider_Clean", "ContractTitle_Clean", "ContractNumber_Clean", "InvoiceDate_Clean", "InvoiceAmount_Clean",
         "RiskScore", "DataQualityRisk", "ContractRisk", "FinancialRisk"],
        "Risk Scores",
    )

    annual = validate_columns(
        annual,
        ["Provider_Clean", "ContractTitle_Clean", "ContractNumber_Clean", "Year", "AnnualSpend", "ComplianceFlag"],
        "Annual Summary",
    )

    monthly = validate_columns(
        monthly,
        ["Provider_Clean", "ContractTitle_Clean", "ContractNumber_Clean", "Year", "Month", "MonthlySpend", "ComplianceFlag"],
        "Monthly Summary",
    )

    monthly_forecast = validate_columns(
        monthly_forecast,
        ["Provider_Clean", "ContractTitle_Clean", "ContractNumber_Clean", "ds", "ForecastSpend", "yhat_lower", "yhat_upper"],
        "Monthly Forecast",
    )

    annual_forecast = validate_columns(
        annual_forecast,
        ["Provider_Clean", "ContractTitle_Clean", "ContractNumber_Clean", "Year", "AnnualForecastSpend"],
        "Annual Forecast",
    )

    kpis = validate_columns(
        kpis,
        ["Metric", "Value"],
        "KPIs",
    )

    scenario = validate_columns(
        scenario,
        ["Scenario", "TotalSpend"],
        "Scenario Modeling",
    )

    # 3. Export curated CSVs with stable schemas
    export_curated(
        cleaned,
        POWERBI_INVOICES_PATH,
        ["Provider_Clean", "ContractTitle_Clean", "ContractNumber_Clean", "InvoiceDate_Clean", "InvoiceAmount_Clean"],
    )

    export_curated(
        risks,
        POWERBI_RISKS_PATH,
        ["Provider_Clean", "ContractTitle_Clean", "ContractNumber_Clean", "InvoiceDate_Clean", "InvoiceAmount_Clean",
         "RiskScore", "DataQualityRisk", "ContractRisk", "FinancialRisk"],
    )

    export_curated(
        annual,
        POWERBI_ANNUAL_PATH,
        ["Provider_Clean", "ContractTitle_Clean", "ContractNumber_Clean", "Year", "AnnualSpend", "ComplianceFlag"],
    )

    export_curated(
        monthly,
        POWERBI_MONTHLY_PATH,
        ["Provider_Clean", "ContractTitle_Clean", "ContractNumber_Clean", "Year", "Month", "MonthlySpend", "ComplianceFlag"],
    )

    export_curated(
        monthly_forecast,
        POWERBI_FORECAST_MONTHLY_PATH,
        ["Provider_Clean", "ContractTitle_Clean", "ContractNumber_Clean", "ds", "ForecastSpend", "yhat_lower", "yhat_upper"],
    )

    export_curated(
        annual_forecast,
        POWERBI_FORECAST_ANNUAL_PATH,
        ["Provider_Clean", "ContractTitle_Clean", "ContractNumber_Clean", "Year", "AnnualForecastSpend"],
    )

    export_curated(
        kpis,
        POWERBI_KPIS_PATH,
        ["Metric", "Value"],
    )

    export_curated(
        scenario,
        POWERBI_SCENARIO_PATH,
        ["Scenario", "TotalSpend"],
    )


if __name__ == "__main__":
    main()

"""
Analysis Module

This script runs after `contract_summary.py` to generate advanced analytics:
    - Forecasting monthly and annual spend per provider/contract (using Prophet).
    - KPI calculations for procurement performance.
    - Scenario modeling for supplier consolidation (using clustering).

Outputs (for Power BI integration):
    - Monthly forecast: DEFAULT_ANALYSIS_MONTHLY_FORECAST_PATH
    - Annual forecast: DEFAULT_ANALYSIS_ANNUAL_FORECAST_PATH
    - KPIs: DEFAULT_ANALYSIS_KPIS_PATH
    - Scenario modeling: DEFAULT_ANALYSIS_SCENARIO_PATH
"""

import pandas as pd
import numpy as np
from typing import Dict
from prophet import Prophet
from sklearn.cluster import KMeans

from constants import *


# ---------------------------------------------------------------------------
# Forecasting with Time-Series Modeling
# ---------------------------------------------------------------------------
def forecast_monthly_spend(monthly_df: pd.DataFrame, forecast_periods: int = 12) -> pd.DataFrame:
    """
    Forecast monthly spend per provider/contract using Prophet.

    Args:
        monthly_df (pd.DataFrame): Monthly spend dataset with Year, Month, MonthlySpend.
        forecast_periods (int): Number of months to forecast into the future.

    Returns:
        pd.DataFrame: Forecasted monthly spend including yhat_lower and yhat_upper.
    """
    all_forecasts = pd.DataFrame()

    # Prepare datetime column safely
    try:
        monthly_df = monthly_df.copy()
        monthly_df["Year"] = pd.to_numeric(monthly_df["Year"], errors="coerce")
        monthly_df["Month"] = pd.to_numeric(monthly_df["Month"], errors="coerce")
        monthly_df["ds"] = pd.to_datetime(
            monthly_df["Year"].astype("Int64").astype(str) + "-" +
            monthly_df["Month"].astype("Int64").astype(str) + "-01",
            errors="coerce"
        )
        monthly_df = monthly_df.dropna(subset=["ds", "MonthlySpend"])
        monthly_df = monthly_df.rename(columns={"MonthlySpend": "y"})
    except Exception as e:
        print(f"[Warning] Failed to prepare time-series data: {e}")
        return all_forecasts

    # Group by provider/contract including ContractNumber_Clean
    for (provider, contract, contract_number), group in monthly_df.groupby(
        ["Provider_Clean", "ContractTitle_Clean", "ContractNumber_Clean"]
    ):
        if len(group) < 3:
            continue
        try:
            model = Prophet(yearly_seasonality=True, weekly_seasonality=False, daily_seasonality=False)
            model.fit(group[["ds", "y"]])
            future = model.make_future_dataframe(periods=forecast_periods, freq="MS")
            forecast = model.predict(future)

            forecast["Provider_Clean"] = provider
            forecast["ContractTitle_Clean"] = contract
            forecast["ContractNumber_Clean"] = contract_number

            all_forecasts = pd.concat(
                [all_forecasts, forecast[[
                    "ds", "yhat", "yhat_lower", "yhat_upper",
                    "Provider_Clean", "ContractTitle_Clean", "ContractNumber_Clean"
                ]]],
                ignore_index=True
            )
        except Exception as e:
            print(f"[Warning] Forecast failed for {provider} - {contract} - {contract_number}: {e}")
            continue

    return all_forecasts.rename(columns={"yhat": "ForecastSpend"})


def forecast_annual_spend(monthly_forecast_df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate monthly forecast into annual totals per provider/contract.

    Args:
        monthly_forecast_df (pd.DataFrame): Monthly forecast with ContractNumber_Clean.

    Returns:
        pd.DataFrame: Annual spend forecast.
    """
    if monthly_forecast_df.empty:
        return pd.DataFrame()

    monthly_forecast_df["Year"] = pd.to_datetime(monthly_forecast_df["ds"]).dt.year
    annual_forecast = (
        monthly_forecast_df.groupby(
            ["Provider_Clean", "ContractTitle_Clean", "ContractNumber_Clean", "Year"]
        )
        .agg({"ForecastSpend": "sum"})
        .reset_index()
        .rename(columns={"ForecastSpend": "AnnualForecastSpend"})
    )
    return annual_forecast


# ---------------------------------------------------------------------------
# Advanced KPI Calculations
# ---------------------------------------------------------------------------
def compute_advanced_kpis(monthly_df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute advanced procurement KPIs:
        - Total spend, supplier count, contract count.
        - Average supplier stability (variance-based).
        - Anomaly rate using IQR method.

    Args:
        monthly_df (pd.DataFrame): Monthly spend dataset.

    Returns:
        pd.DataFrame: KPI name-value pairs.
    """
    kpis = []
    try:
        total_spend = monthly_df["MonthlySpend"].sum()
        total_suppliers = monthly_df["Provider_Clean"].nunique()
        total_contracts = monthly_df["ContractTitle_Clean"].nunique()

        spend_std = monthly_df.groupby("Provider_Clean")["MonthlySpend"].std().fillna(0)
        max_std = spend_std.max() if spend_std.max() > 0 else 1
        supplier_stability = 1 - (spend_std / max_std)
        avg_stability_score = supplier_stability.mean()

        q1 = monthly_df['MonthlySpend'].quantile(0.25)
        q3 = monthly_df['MonthlySpend'].quantile(0.75)
        iqr = q3 - q1
        upper_bound = q3 + 1.5 * iqr
        lower_bound = q1 - 1.5 * iqr
        anomalies = monthly_df[(monthly_df['MonthlySpend'] > upper_bound) | (monthly_df['MonthlySpend'] < lower_bound)]
        anomaly_rate = len(anomalies) / len(monthly_df) if len(monthly_df) > 0 else 0

        kpis.extend([
            {"Metric": "TotalSpend", "Value": total_spend},
            {"Metric": "SupplierCount", "Value": total_suppliers},
            {"Metric": "ContractCount", "Value": total_contracts},
            {"Metric": "AvgSupplierStabilityScore", "Value": avg_stability_score},
            {"Metric": "AnomalyRate", "Value": anomaly_rate}
        ])
    except Exception as e:
        print(f"[Warning] Failed to compute KPIs: {e}")

    return pd.DataFrame(kpis)


# ---------------------------------------------------------------------------
# Scenario Modeling with ML Clustering
# ---------------------------------------------------------------------------
def simulate_supplier_consolidation(monthly_df: pd.DataFrame) -> pd.DataFrame:
    """
    Simulate potential savings from supplier consolidation using clustering.

    Args:
        monthly_df (pd.DataFrame): Monthly spend dataset.

    Returns:
        pd.DataFrame: Scenario results (Baseline vs Clustered Consolidation).
    """
    try:
        supplier_spend = monthly_df.groupby("Provider_Clean")["MonthlySpend"].sum().reset_index()
        if supplier_spend.empty or len(supplier_spend) < 3:
            return pd.DataFrame([{"Scenario": "Baseline", "TotalSpend": supplier_spend["MonthlySpend"].sum() if not supplier_spend.empty else 0}])

        kmeans = KMeans(n_clusters=3, random_state=42, n_init=10)
        supplier_spend['Cluster'] = kmeans.fit_predict(supplier_spend[['MonthlySpend']])
        
        cluster_savings: Dict[int, float] = {0: 0.08, 1: 0.05, 2: 0.02}
        baseline_spend = supplier_spend["MonthlySpend"].sum()

        # Calculate the total spend under the consolidated scenario.
        # This is done by iterating through each cluster and applying its specific savings rate.
        consolidated_spend = sum(
            supplier_spend[supplier_spend['Cluster'] == c]['MonthlySpend'].sum() * (1 - cluster_savings.get(c, 0))
            for c in supplier_spend['Cluster'].unique()
        )

        return pd.DataFrame([
            {"Scenario": "Baseline", "TotalSpend": baseline_spend},
            {"Scenario": "Clustered Consolidation", "TotalSpend": consolidated_spend}
        ])
    except Exception as e:
        print(f"[Warning] Scenario modeling failed: {e}")
        return pd.DataFrame()


# ---------------------------------------------------------------------------
# Main Analysis Pipeline
# ---------------------------------------------------------------------------
def main():
    """
    Run full analysis pipeline:
        1. Load monthly summary data.
        2. Run forecasting (monthly + annual).
        3. Compute KPIs.
        4. Simulate supplier consolidation.
        5. Save results to CSV for Power BI.
    """
    try:
        monthly_df = pd.read_csv(DEFAULT_SUMMARY_MONTHLY_PATH)
    except FileNotFoundError:
        print(f"[Error] Monthly summary file not found: {DEFAULT_SUMMARY_MONTHLY_PATH}")
        return
    except Exception as e:
        print(f"[Error] Failed to load monthly summary: {e}")
        return

    # Exclude mismatched contracts for forecasting
    filtered_df = monthly_df[monthly_df['ComplianceFlag'] != 'ContractMismatch'].copy()

    # Forecasting
    monthly_forecast_df = forecast_monthly_spend(filtered_df)
    if not monthly_forecast_df.empty:
        monthly_forecast_df.to_csv(DEFAULT_ANALYSIS_MONTHLY_FORECAST_PATH, index=False)
        print(f"Monthly forecast saved to {DEFAULT_ANALYSIS_MONTHLY_FORECAST_PATH}")

        annual_forecast_df = forecast_annual_spend(monthly_forecast_df)
        if not annual_forecast_df.empty:
            annual_forecast_df.to_csv(DEFAULT_ANALYSIS_ANNUAL_FORECAST_PATH, index=False)
            print(f"Annual forecast saved to {DEFAULT_ANALYSIS_ANNUAL_FORECAST_PATH}")

    # KPIs
    kpis_df = compute_advanced_kpis(monthly_df)
    if not kpis_df.empty:
        kpis_df.to_csv(DEFAULT_ANALYSIS_KPIS_PATH, index=False)
        print(f"KPIs saved to {DEFAULT_ANALYSIS_KPIS_PATH}")

    # Scenario modeling
    scenario_df = simulate_supplier_consolidation(monthly_df)
    if not scenario_df.empty:
        scenario_df.to_csv(DEFAULT_ANALYSIS_SCENARIO_PATH, index=False)
        print(f"Scenario model saved to {DEFAULT_ANALYSIS_SCENARIO_PATH}")


if __name__ == "__main__":
    main()


"""
Constants Module

Defines absolute paths for data and Power BI exports,
so the project works regardless of the working directory.
"""

import os

# Project root = one level up from this file (src -> dashboard)
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

# Data directories
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
POWERBI_DIR = os.path.join(PROJECT_ROOT, "powerbi_data")

# Raw + cleaned data
DEFAULT_CONTRACT_PATH = os.path.join(DATA_DIR, "contracts.json")
DEFAULT_INVOICE_PATH = os.path.join(DATA_DIR, "simulated_transactions.csv")
DEFAULT_CLEANED_PATH = os.path.join(DATA_DIR, "cleaned_procurement_data.csv")

# Generation parameters
DEFAULT_UNIT = 1000000
DEFAULT_POSSIBILITY = 0.05

# Risk scoring
DEFAULT_RISK_PATH = os.path.join(DATA_DIR, "supplier_risk.csv")

# Supplier summaries
DEFAULT_SUMMARY_ANNUAL_PATH = os.path.join(DATA_DIR, "supplier_annual_summary.csv")
DEFAULT_SUMMARY_MONTHLY_PATH = os.path.join(DATA_DIR, "supplier_monthly_summary.csv")

# Forecasts + analysis outputs
DEFAULT_ANALYSIS_MONTHLY_FORECAST_PATH = os.path.join(DATA_DIR, "monthly_forecast.csv")
DEFAULT_ANALYSIS_ANNUAL_FORECAST_PATH = os.path.join(DATA_DIR, "annual_forecast.csv")
DEFAULT_ANALYSIS_KPIS_PATH = os.path.join(DATA_DIR, "kpis.csv")
DEFAULT_ANALYSIS_SCENARIO_PATH = os.path.join(DATA_DIR, "scenario_model.csv")

# Power BI export paths
POWERBI_INVOICES_PATH = os.path.join(POWERBI_DIR, "invoices.csv")
POWERBI_RISKS_PATH = os.path.join(POWERBI_DIR, "risks.csv")
POWERBI_ANNUAL_PATH = os.path.join(POWERBI_DIR, "annual_summary.csv")
POWERBI_MONTHLY_PATH = os.path.join(POWERBI_DIR, "monthly_summary.csv")
POWERBI_FORECAST_MONTHLY_PATH = os.path.join(POWERBI_DIR, "monthly_forecast.csv")
POWERBI_FORECAST_ANNUAL_PATH = os.path.join(POWERBI_DIR, "annual_forecast.csv")
POWERBI_KPIS_PATH = os.path.join(POWERBI_DIR, "kpis.csv")
POWERBI_SCENARIO_PATH = os.path.join(POWERBI_DIR, "scenario_model.csv")

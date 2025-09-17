# Procurement Analytics Dashboard Workflow

This document explains the **end-to-end data pipeline**, CI/CD workflow, and purpose of each module in the Procurement Analytics Dashboard. The workflow is designed to **automate data preparation, risk scoring, contract compliance analysis, forecasting, KPI computation, scenario modeling, and Power BI export**.

---

## Table of Contents
1. [Overview](#overview)  
2. [CI/CD Pipeline](#cicd-pipeline)  
3. [Module Descriptions](#module-descriptions)  
4. [Running the Project Locally](#running-the-project-locally)  
5. [Power BI Integration](#power-bi-integration)  
6. [File Outputs](#file-outputs)  
7. [Error Handling & Validation](#error-handling--validation)  

---

## Overview

The Procurement Analytics Dashboard collects transaction-level procurement data and transforms it into actionable insights for management.  

Key objectives:
- Ensure **clean, validated, and normalized data**.  
- Compute **risk scores** for supplier contracts.  
- Provide **contract compliance summaries** (annual and monthly).  
- Perform **forecasting, KPI calculations, and scenario modeling**.  
- Export curated datasets for **Power BI dashboards**.  

---

## CI/CD Pipeline

The automated workflow is implemented using **GitHub Actions**, with the following steps:

1. **Data Generation** (`data_generator.py`)  
2. **ETL Processing** (`etl.py`)  
3. **Risk Scoring** (`risk_score.py`)  
4. **Contract Summary** (`contract_summary.py`)  
5. **Advanced Analysis** (`analyse.py`)  
6. **Power BI Export** (`export_powerbi.py`)  
7. **Artifact Upload (CI/CD)**  

**Schedule:** Daily at 8 PM, with manual triggers possible.  

**Key Features:**
- Handles multiple contracts per provider using **Provider + ContractTitle + ContractNumber**.  
- Skips invalid or insufficient data without failing the pipeline.  
- Generates all CSVs required for dashboards automatically.  

---

## Module Descriptions

### data_generator.py
- Generate or refresh mock/real procurement transaction data.
- Output: `data/transactions_raw.csv`

### etl.py
- Clean, normalize, and prepare transaction data.
- Output: `data/transactions_cleaned.csv`

### risk_score.py
- Compute composite risk scores (data quality, contract compliance, financial anomalies).
- Output: `data/risk_scores.csv`

### contract_summary.py
- Compute annual and monthly spend per supplier–contract.
- Includes compliance flags: `WithinBounds`, `OverUpper`, `UnderLower`, `ContractMismatch`.
- Output: `data/supplier_summary_annual.csv`, `data/supplier_summary_monthly.csv`

### analyse.py
- Advanced analytics (forecasting, KPI computation, scenario modeling).
- Outputs: `forecasting_monthly.csv`, `forecasting_annual.csv`, `kpis.csv`, `scenario_model.csv`

### export_powerbi.py
- Consolidate outputs into curated CSVs for Power BI.
- Outputs:
  - `invoices_curated.csv`
  - `risk_scores_curated.csv`
  - `supplier_summary_annual.csv`
  - `supplier_summary_monthly.csv`
  - `forecasting_monthly.csv`
  - `forecasting_annual.csv`
  - `kpis.csv`
  - `scenario_model.csv`

---

## Running the Project Locally

## Create a virtual environment
python3.13 -m venv dashboard

## Activate the virtual environment (Linux)
source dashboard/bin/activate

## Install dependencies
python3.13 -m pip install --upgrade pip
python3.13 -m pip install -r requirements.txt

## Run pipline
bash run_pipeline.sh
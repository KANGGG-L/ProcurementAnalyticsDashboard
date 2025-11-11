#!/bin/bash
# ==============================================================
# Procurement Analytics Dashboard - Pipeline Runner
# Runs the full pipeline in order:
#   1. Data Generation
#   2. ETL Cleaning
#   3. Risk Scoring
#   4. Contract Summary
#   5. Analyse
#   6. Power BI Export
# ==============================================================

# Get absolute project root (directory where this script is located)
PROJECT_ROOT="$(cd "$(dirname "$0")" && pwd)"
SRC_DIR="$PROJECT_ROOT/dashboard/src"

echo "=== Procurement Analytics Pipeline ==="
echo "Project root: $PROJECT_ROOT"

# Step 1: Generate simulated transactions
echo "=== Step 1: Generate data ==="
python3.13 "$SRC_DIR/data_generator.py"
# Check if the last command (data_generator) was successful
if [ $? -ne 0 ]; then
  echo "Error: Step 1 failed. Aborting pipeline."
  exit 1
fi

# Step 2: Run ETL pipeline
echo "=== Step 2: ETL cleaning ==="
python3.13 "$SRC_DIR/etl.py"
# Check if the last command (etl) was successful
if [ $? -ne 0 ]; then
  echo "Error: Step 2 failed. Aborting pipeline."
  exit 1
fi

# Step 3: Risk scoring
echo "=== Step 3: Risk scoring ==="
python3.13 "$SRC_DIR/risk_score.py"
# Check if the last command (risk_score) was successful
if [ $? -ne 0 ]; then
  echo "Error: Step 3 failed. Aborting pipeline."
  exit 1
fi

# Step 4: Contract summaries
echo "=== Step 4: Contract summary ==="
python3.13 "$SRC_DIR/contract_summary.py"
# Check if the last command (contract_summary) was successful
if [ $? -ne 0 ]; then
  echo "Error: Step 4 failed. Aborting pipeline."
  exit 1
fi

# Step 5: Analyse
echo "=== Step 5: Analyse ==="
python3.13 "$SRC_DIR/analyse.py"
# Check if the last command (export_powerbi) was successful
if [ $? -ne 0 ]; then
  echo "Error: Step 5 failed. Aborting pipeline."
  exit 1
fi

# Step 6: Power BI export
echo "=== Step 6: Export for Power BI ==="
python3.13 "$SRC_DIR/export_powerbi.py"
# Check if the last command (export_powerbi) was successful
if [ $? -ne 0 ]; then
  echo "Error: Step 6 failed. Aborting pipeline."
  exit 1
fi

# Step 7: Commit and Push Generated Data
echo "=== Step 7: Committing generated data to repo ==="

# Configure Git identity for the commit
git config user.name "GitHub Actions Bot"
git config user.email "actions@github.com"

# Check for any changes in the dashboard/data and dashboard/powerbi_data directories
git add "$PROJECT_ROOT/dashboard/data/"
git add "$PROJECT_ROOT/dashboard/powerbi_data/"

# Check if there are any files staged to commit
if ! git diff-index --quiet --cached HEAD; then
  COMMIT_MESSAGE="Auto-refresh data and forecasts ($(date +'%Y-%m-%d'))"
  git commit -m "$COMMIT_MESSAGE"
  
  # Push the changes back to the origin branch
  git push
  echo "Successfully committed and pushed new data."
else
  echo "No data changes to commit."
fi


# If all steps succeeded, print success message
echo "=== Pipeline completed successfully! ==="
"""
Procurement Contract Scraper

This module uses Selenium WebDriver to scrape tender data from the City of Melbourne website. 
It extracts structured information about contracts, 
including contract title, service provider, contract number, contract's annual value, and expiry dates.
The scraped data is used as a "seed dataset" for simulating procurement transactions later in the pipeline.

Workflow:
    1. Open the City of Melbourne procurement webpage with Selenium.
    2. Locate and parse the tender table.
    3. Extract fields: title, supplier, contract number, annual value, expiry date.
    4. Convert "annual value" ranges into numeric lower and upper bounds.
    5. Save data into a structured list of `Contract` objects.
    6. Dump saved Contract data into a json file.
"""

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from typing import Optional

import json
from pathlib import Path

from constants import DEFAULT_CONTRACT_PATH, DEFAULT_UNIT

class Contract:
    """
    A class to represent a government contract with specific details.
    
    Attributes:
        contract_title (str): The title of the contract.
        service_provider (str): The name of the company awarded the contract.
        contract_number (str): The unique number associated with the contract.
        annual_value_lower_bound (int/None): Estimated lower bound of contract value.
        annual_value_upper_bound (int/None): Estimated upper bound of contract value.
        expiry_date (str): The contract's expiry date as text.
    """

    def __init__(self, contract_title: str, service_provider: str, contract_number: int,
                annual_value_lower_bound: Optional[int] , annual_value_upper_bound: Optional[int], expiry_date: str):
        """
        Initializes a new Contract object.

        Args:
            contract_title (str): Title of the contract.
            service_provider (str): Supplier or company name.
            contract_number (str): Unique identifier for the contract.
            annual_value_lower_bound (int/None): Lower bound in AUD (if available).
            annual_value_upper_bound (int/None): Upper bound in AUD (if available).
            expiry_date (str): Expiry date as string from source site.
        """
        self.contract_title = contract_title
        self.service_provider = service_provider
        self.contract_number = contract_number
        self.annual_value_lower_bound = annual_value_lower_bound
        self.annual_value_upper_bound = annual_value_upper_bound
        self.expiry_date = expiry_date

    def __repr__(self):
        """
        String representation of the Contract object.
        Useful for debugging and quick inspection.
        """
        return (f"Contract(contract_title='{self.contract_title}', "
                f"service_provider='{self.service_provider}', "
                f"contract_number='{self.contract_number}', "
                f"annual_value_lower_bound='{self.annual_value_lower_bound}', "
                f"annual_value_upper_bound='{self.annual_value_upper_bound}', "
                f"expiry_date='{self.expiry_date}')")

    def to_dict(self) -> dict:
        """
        Convert Contract object to a dictionary for JSON serialization.
        """
        return {
            "contract_title": self.contract_title,
            "service_provider": self.service_provider,
            "contract_number": self.contract_number,
            "annual_value_lower_bound": self.annual_value_lower_bound,
            "annual_value_upper_bound": self.annual_value_upper_bound,
            "expiry_date": self.expiry_date
        }

def parse_annual_value(value_str:str, unit:int = DEFAULT_UNIT) -> tuple:
    """
    Parses the annual value string into numeric lower and upper bounds.

    The source page typically expresses values in "millions", e.g.:
        "$1 to 2 million"
        "Above $2 million"
        "$3 million"

    Args:
        value_str (str): Raw string from the webpage (e.g., "$1 to 2 million").
        unit (int, optional): Base unit multiplier. Defaults to 1,000,000.

    Returns:
        tuple: (lower_bound, upper_bound) as integers in AUD,
               or (None, None) if parsing fails.
    """
    clean_str = value_str.replace('$', '').replace('million', '').strip()
    lower_bound, upper_bound = None, None

    if 'to' in clean_str:
        # Range case: "$1 to 2 million"
        try:
            parts = clean_str.split(' to ')
            lower_bound = int(float(parts[0]) * unit)
            upper_bound = int(float(parts[1]) * unit)
        except (ValueError, IndexError):
            pass

    elif 'Above' in clean_str:
        # Open-ended range: "Above $2 million"
        try:
            number_str = clean_str.replace('Above', '').strip()
            lower_bound = int(float(number_str) * unit)
            upper_bound = None
        except ValueError:
            pass

    else:
        # Single fixed value: "$3 million"
        try:
            value = int(float(clean_str) * unit)
            lower_bound, upper_bound = value, value
        except ValueError:
            pass

    return lower_bound, upper_bound


def scrape(url: str ="https://www.melbourne.vic.gov.au/current-contracts-and-future-tenders"):
    """
    Scrapes contracts and tenders from the City of Melbourne procurement page.

    Args:
        url (str, optional): URL of the procurement page. Defaults to Melbourne tenders page.

    Returns:
        list[Contract]: List of Contract objects with extracted details.
    """
    # Set up Selenium with ChromeDriver
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
    contract_list = []

    try:
        print(f"Opening browser and navigating to {url}")
        driver.get(url)

        # Wait for tender table to
        tender_table = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CLASS_NAME, 'cb-table'))
        )
        rows = tender_table.find_elements(By.TAG_NAME, "tr")

        for row in rows:
            cols = row.find_elements(By.TAG_NAME, "td")

            # Expecting 5 columns: Title, Supplier, Number, Value, Expiry
            if len(cols) == 5:
                contract_title = cols[0].text.strip()
                service_provider = cols[1].text.strip()
                contract_number = cols[2].text.strip()
                annual_value_lower_bound, annual_value_upper_bound = parse_annual_value(cols[3].text.strip())
                expiry_date = cols[4].text.strip()

                # Create Contract object and append to list
                contract = Contract(
                    contract_title, service_provider, contract_number,
                    annual_value_lower_bound, annual_value_upper_bound, expiry_date
                )
                contract_list.append(contract)

        if contract_list:
            print(f"Successfully scraped {len(contract_list)} contracts.")

    finally:
        # Ensure browser closes even if scraping fails
        driver.quit()

    return contract_list

if __name__ == "__main__":
    Path("data").mkdir(exist_ok=True)

    contract_list = scrape()
    serializable_contracts = [c.to_dict() for c in contract_list]

    # Save to JSON for later use in transaction generator
    with open(DEFAULT_CONTRACT_PATH, "w") as f:
        json.dump(serializable_contracts, f, indent=4)

    print(f"Contracts saved to {DEFAULT_CONTRACT_PATH}")
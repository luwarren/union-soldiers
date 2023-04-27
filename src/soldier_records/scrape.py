import os
from src.auth import clear_terminal

from src.soldier_records.ids import scrape_soldier_ids
from src.soldier_records.records import get_and_save_soldier_records as soldier_records
from src.soldier_records.compile_csv import compile_soldier_csv

from src.retry_requests import retry_requests

def scrape():
    # print("Scraping soldier ids...")
    # soldier_ids = scrape_soldier_ids()
    # print(f"Scraped {len(soldier_ids)} soldier ids")

    # # scrape soldier records
    # print("Scraping soldier records...")
    # retry_requests(
    #     records_dir = "data/soldier_records",
    #     failed_ids_path = "data/failed_soldier_ids.txt",
    #     ids = soldier_ids,
    #     get_and_save_records = soldier_records,
    # )
    # print("Finished scraping soldier records")

    # compile all scraped records
    print("Compiling scraped records...")
    compile_soldier_csv()
    print("Finished compiling scraped records\n")
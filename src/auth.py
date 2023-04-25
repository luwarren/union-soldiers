import os

from selenium.webdriver.support.wait import WebDriverWait
from seleniumwire import webdriver
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

import src.login as login

REQ_HEADERS = {}

def clear_terminal():
    if os.name == 'nt':
        os.system('cls')
    else:
        os.system('clear')

def get_headers_dict():
    driver = webdriver.Chrome()
    driver.get("https://www.fold3.com/login")

    # Escape out of popup - if on US VPN, it is unnecessary
    try:
        driver.find_element(By.XPATH, '/html/body/div[2]/div[2]/div[1]/div[1]/div/div/div[2]/div[6]/button').click()
    except Exception:
        pass

    # Find email and password input fields to enter values
    email_input = driver.find_element(By.XPATH, '//*[@id="username"]')
    email_input.send_keys(login.EMAIL)
    password_input = driver.find_element(By.XPATH, '//*[@id="password"]')
    password_input.send_keys(login.PASSWORD)

    # Enter information
    driver.find_element(By.XPATH, '//*[@id="signin-btn"]').click()
    
    # wait to be redirected to the home page
    WebDriverWait(driver, 60).until(   
        EC.title_contains("Historical"),
        "Authentication Did Not Succeed Before Timeout"
    )

    reqs = driver.requests
    driver.quit()
 
    clear_terminal()

    # get the last request that has a cookie (the authenticated request)
    reqs_with_cookies = list(filter(lambda req: "cookie" in req.headers , reqs))
    req_header = list(map(lambda req: req.headers, reqs_with_cookies))[-1]

    # create a dictionary of headers
    req_dict = {key: value for key, value in req_header.raw_items()}
    global REQ_HEADERS
    REQ_HEADERS = req_dict

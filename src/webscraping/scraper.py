from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from webdriver_manager.chrome import ChromeDriverManager

import os
import shutil
import requests
import time
import pandas as pd
import base64

def scrape_page(driver, unwanted_texts):
    # Exclude elements with specific classes or ids
    excluded_elements = driver.find_elements(By.CSS_SELECTOR, ".carousel__list img")
    excluded_urls = [elem.get_attribute('src') for elem in excluded_elements]

    product_containers = driver.find_elements(By.XPATH, "//div[@class='styles__StyledTiledContent-dvv1wj-3 bcglTg']")


    products = []
    prices = []
    images = []

    for container in product_containers:
        product_name_elements = []
        price_elements = []
        image_elements = []
        # Check if the product is in stock
        if not container.find_elements(By.XPATH, ".//div[contains(text(), 'out of stock')]"):
            product_name_elements = container.find_elements(By.XPATH, ".//a[contains(@class, 'ddsweb-link__anchor')]/span[@class='styled__Text-sc-1i711qa-1 xZAYu ddsweb-link__text']")
            price_elements = container.find_elements(By.XPATH, ".//p[@class='styled__StyledHeading-sc-119w3hf-2 jWPEtj styled__Text-sc-8qlq5b-1 lnaeiZ beans-price__text']")
            image_elements = container.find_elements(By.XPATH, ".//div[@class='styled__DietaryLogoWithProductImageWrapper-y4x4kn-4 RWcFl']//img[contains(@class, 'ddsweb-responsive-image__image')]")

            for img in image_elements:
                image_src = img.get_attribute('src')
                # Check if src starts with 'data:image', indicating an invalid image
                if not image_src.startswith('data:image'):
                    print(image_src)

        if product_name_elements and price_elements and image_elements:
            product_name = product_name_elements[0].text
            price = price_elements[0].text
            # Filter out invalid image URLs
            valid_image_sources = [img.get_attribute('src') for img in image_elements if not img.get_attribute('src').startswith('data:image')]
            if valid_image_sources:
                image_src = valid_image_sources[0]

            if product_name not in unwanted_texts and valid_image_sources:
                products.append(product_name)
                prices.append(price)
                images.append(image_src)

    return products, prices, images

def download_image(url, filename):
    if url.startswith('http://') or url.startswith('https://'):
        # Standard image URL
        response = requests.get(url, stream=True)
        with open(filename, 'wb') as out_file:
            shutil.copyfileobj(response.raw, out_file)
        del response
    elif url.startswith('data:image'):
        None
        with open(filename, 'wb') as out_file:
            out_file.write(img_data)

def scroll_down(driver):
    # Scroll down multiple times to trigger loading of more images
    for _ in range(5):  # Adjust the number of times to scroll as needed
        driver.find_element(By.TAG_NAME, 'body').send_keys(Keys.PAGE_DOWN)
        time.sleep(1)  # Adjust timing as necessary

def scraper():
    options = Options()
    options.add_experimental_option('detach', True)
    options.binary_location = "/sbin/google-chrome-stable"
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
    
    all_products = []
    all_prices = []
    all_images = []
    unwanted_texts = ["Skip to main content", "Skip to search", "Skip to basket", "Register", "Sign in", "Contact us", "Help"]

    base_url = "https://www.tesco.com/groceries/en-GB/shop/fresh-food/all"
    page_number = 1

    while True:
        url = f"{base_url}?page={page_number}"
        driver = None
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
        driver.get(url)

        # Check for 404 error
        error_elements = driver.find_elements(By.CLASS_NAME, "error-container")
        if error_elements:
            print("404 Error encountered. Stopping scraping.")
            driver.quit()
            break

        scroll_down(driver)
        products, prices, images = scrape_page(driver, unwanted_texts)

        all_products.extend(products)
        all_prices.extend(prices)
        all_images.extend(images)

        driver.quit()
        page_number += 1
        time.sleep(1)  # Adjust timing as necessary

    if not os.path.exists('images'):
       os.makedirs('images')

    for idx, image_url in enumerate(all_images):
        download_image(image_url, f'images/product_{idx}.png')

    df = pd.DataFrame({'Product': all_products, 'Price': all_prices, 'Image': [f'product_{idx}.png' for idx in range(len(all_images))]})   
    df.to_parquet('../../data/tesco_scrape.parquet.gzip')
    df.to_csv('../../data/tesco_scrape.csv')

    df = pd.read_parquet('tesco_scrape.parquet.gzip')

scraper()

import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.by import By
import time

class Selenium:

    def __init__(self, path, url) -> None:
        self.service = Service()
        self.prefs = {"download.default_directory" : path}
        self.options = webdriver.ChromeOptions()
        self.options.add_experimental_option('detach', True)
        self.options.add_experimental_option('prefs', self.prefs)
        self.driver = webdriver.Chrome(service=self.service, options=self.options)
        self.url = url

    def run_selenium(self) -> None:
        self.driver.get(self.url)
        element = self.driver.find_element(By.ID, 'segment')
        element = Select(element)
        element.select_by_value('2')
        time.sleep(1)
        download = self.driver.find_elements(By.TAG_NAME, 'a')
        download[5].click()
        time.sleep(5)
        self.driver.quit()


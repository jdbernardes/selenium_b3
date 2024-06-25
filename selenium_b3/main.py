import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.by import By
import time

##URL for chromedriver: https://developer.chrome.com/docs/chromedriver/downloads

service = Service()
options = webdriver.ChromeOptions()
prefs = {"download.default_directory" : r"C:\Users\I857413\Desktop\TechSkills\FIAP Aulas\TechChallenge\selenium_b3\selenium_b3\data"}
options.add_experimental_option('detach', True)
options.add_experimental_option('prefs', prefs)
driver = webdriver.Chrome(service=service, options=options)



url = 'https://sistemaswebb3-listados.b3.com.br/indexPage/day/ibov?language=pt-br'
driver.get(url)
element=driver.find_element(By.ID, 'segment')
element=Select(element)
element.select_by_value('2')
time.sleep(1)
#Na pagina da B3 não foi definido classe ou ID para a tag a de download nem as proximas páginas 1,2,3,4,5
#Então usando find_elements, o link do download vai ser no index 5
download = driver.find_elements(By.TAG_NAME, 'a')
download[5].click()
time.sleep(5)
driver.quit()
# BludScalling Team:
# - rizki@bahasa.tech modelling part  
# - dzaki@bahasa.tech inference optimization 
# - athifz@bahasa.tech data gathering and preprocessing

# This code is for scraping text from liputan6.com/read/{id}
# It uses beautiful soup to parse the html and csv to write the output
# It uses multiprocessing to parallelize the requests and user agents to avoid detection
# It loops from init_id to latest_id and skips any errors

import requests
import csv
from bs4 import BeautifulSoup
from multiprocessing import Pool
from random import choice
import time

url_base = "liputan6.com/read" # end with some int for its db id
latest_id = 4056342 # int for url base
class_ = "article-content-body" 
processes = 512 # number of processes to use
batch_size = 2048 # number of urls to process in each batch
user_agents = [ # list of user agents to choose from
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux i686; rv:110.0) Gecko/20100101 Firefox/110.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 15_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148",
    "Mozilla/5.0 (iPad; CPU OS 12_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148"
]

# A function to scrape a single url and return the text
def scrape_url(url):
    try:
        # Get the response from the url with a random user agent
        headers = {"User-Agent": choice(user_agents)}
        response = requests.get(url, headers=headers)
        # Check if the response is successful
        if response.status_code == 200:
            # Parse the html with beautiful soup
            soup = BeautifulSoup(response.text, "html.parser")
            # Find the element with the class name
            content = soup.find(class_=class_["body"])
            content = content if content else None


            # Check if the element exists
            if content:
                # Get all the text inside p tags
                text = " ".join(p.text for p in content.find_all("p"))
                
                # Return the url and text as a tuple
                return (url, text)
            
        elif response.status_code == 502:
            print(f"Bad gateway at {url}")
            return None
    except Exception as e:
        # Skip any errors and print them
        print(f"Error at {url}: {e}")

# A function to scrape a batch of urls and write the results to a csv file

def scrape_batch(urls):
    # Create a pool of processes
    pool = Pool(processes)
    # Map the scrape_url function to the urls and get the results
    results = pool.map(scrape_url, urls)
    # Close the pool and wait for the processes to finish
    pool.close()
    pool.join()
    # Create a csv file to store the results
    with open("./blud.csv", "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        # Write the results to the csv file
        for result in results:
            if result: # check if the result is not None
                writer.writerow(result)

# Loop from init_id to latest_id
while latest_id > 0:
    timer = time.time()
    # Create a list of urls to scrape in the current batch
    urls = [f"https://{url_base}/{i}" for i in range(latest_id, latest_id - batch_size, -1)]
    # Scrape the batch of urls
    scrape_batch(urls)
    # Decrement the latest_id by the batch_size
    latest_id -= batch_size
    elapse = time.time() - timer
    print(f"Scraped {batch_size} urls from {latest_id} to {latest_id - batch_size} in {elapse:.2f} seconds")


"""
datasets: 

finetuning asymtric semantic similarity berisi 500 ribu pasang dokument berita liputan6, 200 jt kata hasil scrapping oleh BludScalling Team 
url = https://storage.googleapis.com/simpel/blud/lip6.csv


finetuneing lebih lanjut asymentric for passage retrieval berisi 200 ribu dokumen-ringkasan liputan6 oleh Fajri Koto et. al


url = https://storage.googleapis.com/simpel/blud/lip6.tar.gz

"""
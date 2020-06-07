# This is a webcrawler which can extract reviews posted on an Insurance aggregator website named Policybazaar.com
# On the website variety of Insurance Products are available and reviews/comments are posted by customers.
# This webcrawler can scan different review pages on this site and save them as output in a csv file.
#
# Developer : Tanmay Bhardwaj
# Dated : 4 June 2020

# Import the packages and methods for webcrawler
from bs4 import BeautifulSoup
import pandas as pd
import requests

#Import the necessary methods from tweepy library
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from datetime import datetime
from tweepy import Stream
import tweepy as tw

# Arrays and Lists
insurance_sub_category_list = list()
reviewer_location_list = list()
#company_url_list = list()
review_text_list = list()
review_date_list = list()
review_url_list = list()
product_list = list()
#
reviewer_array = []

# Dataframes
df_insurance_category_reviews = pd.DataFrame()

# Constants and Variables
junk_url='javascript:void(0);'
no_reviews='(Based on 0 Reviews)'
review_exists=''

# Function to reach extract reviews from the webpage/url provided as input parameter.
# It will return review details in four separate lists which will have company product name,
# review text, review date and location of the reviewer.
def get_reviews(url):
    reviews_url = requests.get(url, headers=headers)
    # Use BeautifulSoup to parse HTML page
    reviews_soup = BeautifulSoup(reviews_url.content, "html.parser")
    # Get Company Name
    company = reviews_soup.find('h1')
    # Get Reviewer Information
    reviewer_info = reviews_soup.findAll('div', {"class": "rater-info"})
    # Get all reviews for the company
    all_reviews = reviews_soup.findAll('div', {"class": "revie-descr"})

    # Loop through each review
    for review_class in all_reviews:
        review = review_class.findAll('p')    
        for i in review:
            review_text = i.text        
            # remove words ' Reviews & Rating' from company name
            insurance_category = company.text  
            # Save Company Product
            product_list.append(insurance_category)
            # Save Review Text
            review_text_list.append(review_text)        

    # Loop through each reviewer and save Review Date and reviewer location
    for reviewer_class in reviewer_info:
        reviewer = reviewer_class.get_text(separator = '\n')    
        countX = count_digit(reviewer)    
        if countX > 2:
        # Add rater Location and Review Date to Array    
            reviewer_array.append(reviewer)    

    # Loop through the reviewer Array, extract Reviewer Location and Date of Review and save to the list
    for data in reviewer_array:
        reviewer_data = data
        # Get rater location
        try:
            reviewer_location = reviewer_data.splitlines()[0]
        except IndexError:
            reviewer_location = 'Data Error'    

        # Get date of review
        try:
            review_date = reviewer_data.splitlines()[1]
        except IndexError:
            review_date = 'Data Error'    
       
        # On few instances, review location is not populated, so shifting the position of review date.
        if review_date == 'Data Error':
            review_date = reviewer_data.splitlines()[0]
        else:
            review_date = reviewer_data.splitlines()[1]

        # Save Review Date
        review_date_list.append(review_date)
        # Save Reviewer Location
        reviewer_location_list.append(reviewer_location)
        print('url')
        print(url)
            
    return(product_list,review_text_list,review_date_list,reviewer_location_list)

# Function to check if a string has digits in it, needed to separate review date and user location
# as they are present in same HTML tag
def count_digit(input_string):
    digit_count = 0
    digit_count = sum(list(map(lambda x:1 if x.isdigit() else 0,set(input_string))))
    return(digit_count)

# Set home page URL of the website
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.71 Safari/537.36'}
site_url =  'https://www.policybazaar.com/'    
site_url_request = requests.get(site_url, headers=headers)

# Use BeautifulSoup to parse HTML page
home_page=BeautifulSoup(site_url_request.content, "html.parser")
# Collect URLs of all categories of Insurance
all_insurance = home_page.findAll('div', {"class": "policynav"})
for li in all_insurance:
    for link in li.findAll('a'):
        # Below condition is set to handle garbage data
        if link.get('href')!=junk_url:
            insurance_sub_category_list.append(link.get('href'))
    
# Read all Sub Category URLs and collect URLs of review pages
for url in insurance_sub_category_list:
    sub_category_request = requests.get(url, headers=headers)
    sub_category_url_layout = BeautifulSoup(sub_category_request.content, "html.parser")
    ratings = sub_category_url_layout.find('div', {"class": "based_rating"})
    # Check if any reviews are written for insurance sub-category
    if ratings is not None:
        review_exists =  (sub_category_url_layout.find('div', {"class": "based_rating"})).text
        # If reviews exist then save the URL 
        if (review_exists != no_reviews): 
            review_url = (sub_category_url_layout.find('a', string="Read All Reviews"))
            review_url_list.append(review_url.get('href'))

    #company_info = sub_category_url_layout.findAll('div', {"class": "card insurers"}) 
    # Retrieve Company name and review page URL
    #for li in company_info:
        #for link in li.findAll('a'):        
            #company_url_list.append(link.get('href'))    

# Now we have URLs of each company, we need to get URL of their review page
#for company in company_url_list:
    #print(company)
    # For the company verify if reviews are written, if they exist then only save the URL of the review page
    #company_page = requests.get(company, headers=headers)
    #company_page_layout = BeautifulSoup(company_page.content, "html.parser")
    #rating_url = company_page_layout.find('div', {"class": "based_rating"})
    # Check if any reviews are written for the company
    #if rating_url is not None:
        #review_exists =  (company_page_layout.find('div', {"class": "based_rating"})).text
        # If reviews exist then save the URL 
        #if (review_exists != no_reviews): 
            #review_url = (company_page_layout.find('a', string="Read All Reviews"))
            #review_url_list.append(review_url.get('href'))

# Loop through all the Review URLs, get review details and save them in a csv file
for url in (review_url_list):
    print(url)
    product_list, review_text_list, review_date_list, reviewer_location_list = get_reviews(url)  
    reviews_df = pd.DataFrame(zip(product_list, review_date_list, reviewer_location_list, review_text_list),columns=['Company','Review Date','Reviewer Location','Review Text'])       
    df_insurance_category_reviews = df_insurance_category_reviews.append(reviews_df, ignore_index = True) 
    # Save reviews to a csv file
    df_insurance_category_reviews.to_csv('policy_bazaar_service_reviews.csv')
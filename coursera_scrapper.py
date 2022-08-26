from bs4 import BeautifulSoup
import httplib2
import numpy as np
import pandas as pd
from pathlib import Path
import xml.etree.ElementTree as ET

http = httplib2.Http()

def run_extracting_pipeline():

    all_reviews_dataset = []

    courses_list = get_courses_list()

    for course in courses_list:
        print(course)
        all_reviews_dataset.append(run_pages(course))
        print(all_reviews_dataset)

    all_reviews_dataset = list(np.concatenate(all_reviews_dataset).flat)
    return all_reviews_dataset

def get_courses_list():
    courses_list_xml = 'https://www.coursera.org/sitemap~www~courses.xml'

    status, response = http.request(courses_list_xml)

    all_courses_url = BeautifulSoup(response, "html.parser").findAll('loc')

    for i, el in enumerate(all_courses_url):
        all_courses_url[i] = el.get_text()

    return all_courses_url

def run_pages(course_url):

    page_num = 1
    all_reviews_final = []

    while True:
        base_url = course_url+'/reviews?page='+str(page_num)+'&star=1'

        status, response = http.request(base_url)

        soup = BeautifulSoup(response, "html.parser")

        all_reviews = soup.find_all("div", attrs={"class": "review-page-review"}) #get a full list of reviews in a page

        if len(all_reviews) == 0:
            print(all_reviews)
            print(page_num)

            if len(all_reviews_final) > 0:
                all_reviews_final = list(np.concatenate(all_reviews_final).flat)
            return all_reviews_final
            break
        else:
            print(page_num)
            all_reviews_final.append(read_page(all_reviews, course_url))
            page_num+=1


def read_page(reviews_list, course_url):
    reviews_final = []

    for element in reviews_list:

        course_name = course_url.split('/')[-1]

        single_review = element.findChildren('div',
                                                    recursive=False)  # merge the review in two parts: the review itself and the votes

        review_data = single_review[0].findChildren('div',
                                                    recursive=False)  # merge the review in two parts: name/date and review text

        review_author, review_post_date = review_data[0].findChildren('p', recursive=False)
        review_author = review_author.get_text()
        review_author = review_author.replace("By ", "")
        review_post_date = review_post_date.get_text()

        review_text = review_data[1].findChildren('div')
        review_text = review_text[0].get_text()

        review_votes = single_review[1].findChildren('span')
        review_votes = review_votes[0].get_text()
        review_votes = review_votes.replace("This is helpful (", "")
        review_votes = review_votes.replace(")", "")

        if review_votes == "This is helpful ":
            review_votes = 0

        review_votes = int(review_votes)
        reviews_final.append({'course_name': course_name, 'author': review_author, 'date': review_post_date, 'review_text': review_text, 'votes': review_votes})

    return reviews_final


dataset = pd.DataFrame(run_extracting_pipeline())

filepath = Path('./csv/reviews.csv')
filepath.parent.mkdir(parents=True, exist_ok=True)

dataset.to_csv(filepath)
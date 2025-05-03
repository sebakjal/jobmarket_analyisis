from bs4 import BeautifulSoup
from config import *
from genai_functions import *
import polars as pl
import requests
import time
import random
# from tqdm import tqdm
from datetime import date
from db_functions import *


def name_format(job_name):
    """
    Replace spaces in a job name with URL-safe equivalents.

    Args:
        job_name (str): The job name to modify.

    Returns:
        str: The modified job name with URL-safe spaces.
    """
    return job_name.replace(' ', '%20')

def get_data(url):
    r = requests.get(url, headers={"headers": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"}, timeout=5)

    return BeautifulSoup(r.content, 'html.parser')

def get_jobcards_soup():
    formatted_job_name = name_format(job_name)
    url = f"https://linkedin.com/jobs/search?keywords={formatted_job_name}&location={location}&f_TPR=r{date_posted_in_seconds}"
    return get_data(url)

def get_list_of_jobcards(soup):
    # Parsing the job card info (title, company, location, date, job_url) from the beautiful soup object
    joblist = []
    try:
        divs = soup.find_all('div', class_='base-search-card__info')
    except:
        print("Empty page, no jobs found")
        return joblist
    
    print('Looking for jobs...')
    for item in divs:
        title = item.find('h3').text.strip()
        company = item.find('a', class_='hidden-nested-link')
        location = item.find('span', class_='job-search-card__location')
        parent_div = item.parent
        entity_urn = parent_div['data-entity-urn']
        job_posting_id = entity_urn.split(':')[-1]
        job_url = 'https://www.linkedin.com/jobs/view/'+job_posting_id+'/'

        date_tag_new = item.find('time', class_ = 'job-search-card__listdate--new')
        date_tag = item.find('time', class_='job-search-card__listdate')
        date = date_tag['datetime'] if date_tag else date_tag_new['datetime'] if date_tag_new else ''
        job_description = ''
        job = {
            'title': title,
            'company': company.text.strip().replace('\n', ' ') if company else '',
            'location': location.text.strip() if location else '',
            'date': date,
            'job_url': job_url,
            'job_description': job_description,
        }
        joblist.append(job)

    print(f'Found {len(joblist)} jobs')
    
    return joblist

def get_job_info(soup):

    job_info = {}
    # Get the job description from the job page
    desc_div = soup.find('div', class_='description__text description__text--rich')
    if desc_div:
        # Remove unwanted elements
        for element in desc_div.find_all(['span', 'a']):
            element.decompose()

        # Replace bullet points
        for ul in desc_div.find_all('ul'):
            for li in ul.find_all('li'):
                li.insert(0, '-')

        text = desc_div.get_text(separator='\n').strip()
        text = text.replace('\n\n', '')
        text = text.replace('::marker', '-')
        text = text.replace('-\n', '- ')
        text = text.replace('Show less', '').replace('Show more', '')
        job_info['job_description'] = text
    else:
        job_info['job_description'] = "Could not find Job Description"
    
    # Get the job salary from the job page
    #TODO

    # Get the job contract type from the job page
    # Find the main list container (optional, but good practice if multiple lists exist)
    criteria_list_ul = soup.find('ul', class_='description__job-criteria-list')


    # Check if the main list was found
    if criteria_list_ul:
        # Find all list items within this specific list
        list_items = criteria_list_ul.find_all('li', class_='description__job-criteria-item')

        # Iterate through each list item
        for item in list_items:
            # Find the subheader (h3) for the criterion name
            subheader_tag = item.find('h3', class_='description__job-criteria-subheader')
            # Find the text span for the criterion value
            # Using the more specific class 'description__job-criteria-text--criteria' is slightly safer
            value_tag = item.find('span', class_='description__job-criteria-text--criteria')

            # Ensure both tags were found before extracting text
            if subheader_tag and value_tag:
                # Extract text and clean whitespace (strip removes leading/trailing spaces/newlines)
                criterion_name = subheader_tag.get_text(strip=True).lower().replace(' ', '_')
                criterion_value = value_tag.get_text(strip=True).lower().replace(' ', '_')

                # Add the key-value pair to the dictionary
                job_info[criterion_name] = criterion_value
            else:
                # Optional: Print a warning if the structure is unexpected within an item
                print(f"Warning: Skipping item, couldn't find expected h3/span: {item.prettify()}")

    else:
        print("Error: Could not find the 'ul' with class 'description__job-criteria-list'.")


    return job_info

def enrich_job_list(joblist, keywords):
    """
    Enrich the job list with additional information such as job description, salary, and contract type.

    Args:
        joblist (list): List of job dictionaries.
        keywords (list): List of keywords to filter job titles.

    Returns:
        list: The enriched job list.
    """
    print('Looking for job descriptions...')
    for job in joblist:
        # Check if the job title contains any of the keywords
        if any(keyword in job['title'].lower() for keyword in keywords):
            try:
                print('-' * 30)
                print(f'Getting job description for {job["title"]} in {job["company"]}')

                time.sleep(random.randint(2, 5))

                # Fetch additional job information
                job_info = get_job_info(get_data(job['job_url']))
                job.update(job_info)
                print('Job description starts with:', job_info['job_description'][:10])

            except Exception as e:
                print(f'Error getting job description for {job["title"]} in {job["company"]}: {e}')
    return joblist
    
        
if __name__ == "__main__":

    # Obtiene el objeto soup que contiene las jobcards
    soup = get_jobcards_soup()

    # Devuelve una lista de diccionarios con la información de las jobcards (title, company, location, date, job_url)
    joblist = get_list_of_jobcards(soup)
    
    # Con la info de las jobcards va a la URL de cada una y obtiene detalles del trabajo
    joblist_with_info = enrich_job_list(joblist, keywords)

    # Transform the job list into a DataFrame
    df = pl.DataFrame(joblist_with_info)

    # Show jobs with description
    df = df.filter(pl.col('job_description') != '')
    print(df)
    print(f'Found {len(df)} jobs with description')
        
    # Get today's date
    today = date.today()
    print("Today's date:", today)

    # Export the DataFrame to a Parquet file
    try:
        df.write_parquet(f"/opt/airflow/output/jobs_from_{date_posted_in_days}_days_{today}.parquet")
        print('Exported to Parquet file')

    except Exception as e:
        print(f'Error exporting to Parquet file: {e}')

    # Insert base data into database
    try:
        insert_into_db(df, 'base_table', '/opt/airflow/output/my_project.duckdb', 'base_data')
        print(f'Inserted {len(df)} jobs into base data DB')

    except Exception as e:
        print(f'Error inserting into database: {e}')


    # Use LLM to classify jobs
    try:
        genai_list: list[dict] = []
        for row in df.rows(named=True):
            genai_data: dict = {}
            genai_data.update(classify_job_description(row['job_description']))
            genai_data.update({'job_url': row['job_url']})
            genai_list.append(genai_data)

    except Exception as e:
        print(f'Error classifying jobs: {e}')

    # Put data into Dataframe then insert to DB
    df_genai = pl.DataFrame(genai_list)
    try:
        insert_into_db(df_genai, 'genai_table', '/opt/airflow/output/my_project.duckdb', 'genai_data')
        print(f'Inserted {len(df_genai)} jobs into GenAI data DB')

    except Exception as e:
        print(f'Error inserting GenAI data into database: {e}')
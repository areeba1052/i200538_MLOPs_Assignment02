import logging
import re
from datetime import datetime

import pandas as pd
import requests
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

from airflow import DAG

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def save_to_csv(data):
    if data:
        df = pd.DataFrame(data)
        df.to_csv('/mnt/c/Users/Public/Documents/airflow/dags/data/Data.csv', index=False)
    else:
        logging.info("No data to save.")

def extract_and_transform(**kwargs):
    url = kwargs['url']
    source = kwargs['source']
    selector = kwargs['selector']
    session = requests.Session()
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504, 524])
    session.mount('http://', HTTPAdapter(max_retries=retries))
    session.mount('https://', HTTPAdapter(max_retries=retries))

    try:
        response = session.get(url, timeout=20)
        soup = BeautifulSoup(response.text, 'html.parser')
        links = [a['href'] for a in soup.select(selector) if 'href' in a.attrs]
        links = [url + '/' + link.lstrip('/') if not link.startswith('http') else link for link in links]
        data = []
        for link in links:
            try:
                response = session.get(link, timeout=20)
                article_soup = BeautifulSoup(response.text, 'html.parser')
                title_element = article_soup.find('title')
                title = title_element.text.strip() if title_element else None
                paragraphs = article_soup.find_all('p')
                description = ' '.join(p.text.strip() for p in paragraphs if p.text.strip()) if paragraphs else None

                if title and description:
                    title = re.sub(r'\s+', ' ', re.sub(r'[^\w\s]', '', title)).strip()
                    description = re.sub(r'\s+', ' ', re.sub(r'[^\w\s]', '', description)).strip()
                    data.append({
                        'title': title,
                        'description': description,
                        'source': source,
                        'url': link
                    })
            except requests.exceptions.RequestException as e:
                logging.error(f"Failed to fetch details from {link}: {str(e)}")
        return data
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch {url}: {str(e)}")
        return []

dag = DAG('news_scraping_pipeline', start_date=datetime(2024, 5, 10), schedule_interval="@daily")

with dag:
    source = 'Dawn'
    info = {'url': 'https://www.dawn.com', 'selector': 'article.story a.story__link'}

    setup_git_dvc = BashOperator(
        task_id='setup_git_dvc',
        bash_command=f"""
        cd /mnt/c/Users/Public/Documents/airflow/dags &&
        if [ ! -d ".git" ]; then
            git init &&
            git remote add origin https://github.com/areeba1052/i200538_MLOPs_Assignment02.git;
        fi &&
        if [ ! -d ".dvc" ]; then
            dvc init &&
            dvc remote add -d myremote gdrive://1kEjpfkQt7X-mF3v1WiYN1QCTeMQPMFR9;
        fi
        """
    )
    extract_data = PythonOperator(
        task_id='extract_data',
        python_callable=extract_and_transform,
        op_kwargs=info
    )
    save_data = PythonOperator(
        task_id='save_data',
        python_callable=save_to_csv,
        op_kwargs={'data': extract_data.output},
    )
    control_version_data = BashOperator(
        task_id='control_version_data',
        bash_command="""
        cd /mnt/c/Users/Public/Documents/airflow/dags &&
        dvc add data/dawnData.csv &&
        dvc push &&
        git add data/dawnData.csv.dvc data/.gitignore &&
        git commit -m 'Updated data files' &&
        git push origin master
        """
    )

    setup_git_dvc >> extract_data >> save_data >> control_version_data
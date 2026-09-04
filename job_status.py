import requests
import os
import sys
import pandas as pd
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from dotenv import load_dotenv

def job_post():

    load_dotenv()

    def get_jobs(cursor = None):
        try:
            url = 'https://himalayas.app/jobs/api'
            params = {'limit': 5}
            job_list = []
            if cursor:
                params['cursor'] = cursor
            response = requests.get(url, params = params)
            response.raise_for_status()
            if response.status_code == 200:
                data = response.json()
                for row in data['jobs']:
                   jobs = {
                       'Job Title': row['title'],
                       'Description': row['excerpt'],
                       'Company Name': row['companyName'],
                       'Employment Type': row['employmentType'],
                       'Salary Period': row['salaryPeriod'],
                       'Location': row['locationRestrictions'],
                       'Categories': row['categories'],
                       'Guid': row['guid'],
                       'Apply': row['applicationLink']
                   } 
                   job_list.append(jobs)

            return job_list, data.get('nextCursor')
        
        except Exception as e:
            print(f'Unable to get job details: {e}')

    def extract_all_jobs(max_pages = 5):
        try:
            all_jobs = []
            cursor = None
            page_count = 0

            while page_count <= max_pages:
                jobs, new_cursor = get_jobs(cursor)
                all_jobs.extend(jobs)
                if not new_cursor:
                    break
                else:
                    cursor = new_cursor
                page_count+=1
            df = pd.DataFrame(all_jobs)
            if df.empty:
                print('No Data found in the dataframe')
            else:
                print(f'{len(df)} jobs extracted from the database')
            return df
        except Exception as e:
            print(f'Job Extraction failed: {e}')

    def get_existing_guids():
        try:
            project_id = os.environ['GCP_PROJECT_ID']
            bq_dataset = os.environ['BQ_DATASET']
            bq_table = os.environ['BQ_TABLE']

            client = bigquery.Client(project = project_id)
            table_id = f'{project_id}.{bq_dataset}.{bq_table}'

            try:
                client.get_table(table_id)
                print(f'{bq_table} found in {bq_dataset}')
            except NotFound as nf:
                print(f'Table not found: {nf}')
                return set()

            query = f'SELECT Guid from {table_id}'
            query_job = client.query(query)
            results = query_job.result()

            existing_guids = {row.Guid for row in results}
            print(f'{len(existing_guids)} found in {table_id}')
            return existing_guids

        except Exception as e:
            print(f'Unable to extract guids: {e}')

    def load_new_jobs(df, existing_guids):
        try:
            project_id = os.environ['GCP_PROJECT_ID']
            bq_dataset = os.environ['BQ_DATASET']
            bq_table = os.environ['BQ_TABLE']

            client = bigquery.Client(project = project_id)
            table_id = f'{project_id}.{bq_dataset}.{bq_table}'

            if df.empty:
                print('No Data to load')
                return
            print('performing duplicate check...')
            new_df = df.drop_duplicates(subset=['Guid'])
            if new_df.empty:
                print('No new records to load — all duplicates skipped')
                return
            client = bigquery.Client(project = project_id)
            table_id = f'{project_id}.{bq_dataset}.{bq_table}'

            load_config = bigquery.LoadJobConfig(
                write_disposition = 'WRITE_APPEND',
                autodetect = True
            )

            job_load = client.load_table_from_dataframe(new_df, table_id, job_config = load_config)
            job_load.result()

            table_ref = client.get_table(table_id)
            print(f'Loaded {table_ref.num_rows} rows to the path {table_id}')

        except Exception as e:
            print(f'New Jobs Fetch Failed: {e}')
    def main():
        try:
            print('Fetching GCP Credentials....')
            project_id = os.environ['GCP_PROJECT_ID']
            bq_dataset = os.environ['BQ_DATASET']
            bq_table = os.environ['BQ_TABLE']
        except KeyError as e:
            print(f'Required environment variable is missing: {e}')
            sys.exit(1)

        print('Initiating Pipeline Execution...')
        print('Fetching Job Details to the system...')

        print('Extracting all jobs....')
        df = extract_all_jobs()
        print('Jobs extracted and saved into a dataframe')

        print('Fetching GUIDs from the table.....')
        guids = get_existing_guids()
        print('GUIDs have been recorded successfully')

        print('Loading New Jobs to the database...')
        load_new_jobs(df, guids)
        print('Job load is successful')

        print('Pipeline execution completed successfully')
    
    if __name__ == '__main__':
        main()

job_post()
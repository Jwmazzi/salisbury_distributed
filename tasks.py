from multiprocessing import Pool, cpu_count
from urllib.parse import urlparse
from newspaper import Article
from itertools import chain
from functools import wraps
from celery import Celery
from io import StringIO
import traceback
import sqlite3
import pandas
import boto3
import time
import json
import re


import warnings
warnings.filterwarnings("ignore")


# Read Local Configuration & Define Application
cfg = json.loads(open('config.json').read())
app = Celery('tasks', backend='rpc://', broker=f'amqp://{cfg["username"]}:{cfg["password"]}@{cfg["rabbitmq"]}')


def open_connection(func):

    @wraps(func)
    def wrap(*args, **kwargs):

        con = sqlite3.connect('gdelt.db')
        cur = con.cursor()

        args = list(args)
        args.insert(0, cur)

        try:
            func(*args, **kwargs)
        except:
            print(traceback.format_exc())
        finally:
            con.commit()
            con.close()

    return wrap


def batch_it(l, n):

    n = n if n > 0 else 1

    for i in range(0, len(l), n):
        yield l[i:i + n]


def batch_process_articles(article_list):

    print(f"Subprocess Handling {len(article_list)} Articles")

    processed_data = []

    for event_article in article_list:

        try:
            # Parse GDELT Source
            article = Article(event_article)
            article.download()
            article.parse()
            article.nlp()

            # Unpack Article Properties & Replace Special Characters
            title     = article.title.replace("'", '')
            site      = urlparse(article.source_url).netloc
            summary   = '{} . . . '.format(article.summary.replace("'", '')[:500])
            keywords  = ', '.join(sorted([re.sub('[^a-zA-Z0-9 \n]', '', key) for key in article.keywords]))
            meta_keys = ', '.join(sorted([re.sub('[^a-zA-Z0-9 \n]', '', key) for key in article.meta_keywords]))

            processed_data.append([event_article, title, site, summary, keywords, meta_keys])

        except:
            processed_data.append([event_article, None, None, None, None, None])

    return processed_data


@app.task
def process_csv(csv_key):

    # Define S3 Connection
    s3 = boto3.resource('s3')

    # Read Target CSV into a Data Frame
    base_df = pandas.read_csv(s3.Object('gdelt-geoanalytics', csv_key).get()['Body'])

    # Build Lists to Drive Multiprocessing
    articles = base_df.SOURCEURL.to_list()
    batches = list(batch_it(articles, int(len(articles) / cpu_count() - 1)))

    # Create Pool & Run Records
    pool = Pool(processes=cpu_count() - 1)
    data = pool.map(batch_process_articles, batches)
    pool.close()
    pool.join()

    # Merge Article Data to Original Data Frame
    proc_df = pandas.DataFrame(list(chain(*data)), columns=['SOURCEURL', 'TITLE', 'SITE', 'SUMMARY', 'KEYWORDS', 'META'])
    dump_df = base_df.merge(proc_df, on='SOURCEURL')

    # Remove Empty Placeholder Attributes & Remove Suffix From Newly Merged Attributes
    dump_df = dump_df[[c for c in dump_df.columns if not c.endswith('_x')]]
    dump_df.columns = dump_df.columns.str.replace('_y', '')

    # Push Data Back to S3
    df_buff = StringIO()
    dump_df.to_csv(df_buff)
    s3.Object('gdelt-geoanalytics', csv_key).put(Body=df_buff.getvalue())

    return csv_key


@open_connection
def create_tracking_table(cursor, table_name):

    cursor.execute(f'''
                    create table if not exists {table_name}
                    (key_name text, key_time text)
                    ''')


@open_connection
def insert_result(cursor, table_name, key_name, key_time):

    cursor.execute(f'''
                    insert into {table_name}
                    values ('{key_name}', '{key_time}')
                    ''')


if __name__ == "__main__":

    # SQLite Table Name - E.G. run_1582309726
    db_table = f'{cfg["db_table"]}_{int(time.time())}'

    # Build Base Table for Storing Results
    create_tracking_table(db_table)

    # Connect to S3 Bucket
    bucket = boto3.resource('s3').Bucket('gdelt-geoanalytics')
    keys   = [obj.key for obj in bucket.objects.all() if obj.key.endswith('csv')]

    # Create Tasks on Queue and Store Async Results
    results = [process_csv.delay(key) for key in keys]

    # Wait for All Results to Return From Queue & Write to SQLite
    while len(results) > 1:

        for idx, result in enumerate(results):
            if result.ready():
                insert_result(db_table, result.get(), time.time())
                del results[idx]

            print(f'Pending Jobs: {len(results)}')

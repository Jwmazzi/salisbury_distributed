from multiprocessing import Pool, cpu_count
from urllib.parse import urlparse
from newspaper import Article
from itertools import chain
from celery import Celery
from io import StringIO
import pandas
import boto3
import time
import json
import re
import os


import warnings
warnings.filterwarnings("ignore")


# Define Local Script Location
this_dir = os.path.split(os.path.realpath(__file__))[0]

# Read Local Configuration & Define Application
cfg = json.loads(open(os.path.join(this_dir, 'config.json')).read())
app = Celery('tasks', broker=f'amqp://{cfg["username"]}:{cfg["password"]}@{cfg["rabbitmq"]}')


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

    print(f'Processing Key: {csv_key}')

    process_start = time.time()

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

    # Push GDELT CSV Back to S3
    out_buff = StringIO()
    dump_df.to_csv(out_buff, index=False)
    s3.Object('gdelt-geoanalytics', csv_key).put(Body=out_buff.getvalue())

    # Create Accounting Note - E.G. 2018010.csv_122.txt
    run_time = int((time.time() - process_start) // 60)
    s3.Object('gdelt-geoanalytics', f'completed/{csv_key.split("/")[1]}_{run_time}.txt').put(Body=b' ')

    return csv_key


if __name__ == "__main__":

    # Connect to S3 Bucket & Fetch All CSV Keys
    bucket = boto3.resource('s3').Bucket('gdelt-geoanalytics')
    keys   = [obj.key for obj in bucket.objects.all() if obj.key.endswith('csv')]

    # Push CSV Keys to the Queue
    for key in keys:
        process_csv.delay(key)

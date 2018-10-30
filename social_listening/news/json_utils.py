import os
import json
import argparse
import getpass

from pymongo import MongoClient
from bs4 import BeautifulSoup


DATALOC = '../../data/news/'
KEYWORDS = ['refugee', 'refugees', 'migrant', 'migrants', 'asylum', 'rohingya',
            'immigrant', 'immigrants', 'UNHCR']


def get_jsonfiles(foldername):
    '''Get all files from folder'''
    foldername = os.path.join(DATALOC, foldername)
    return [jsonfile for jsonfile in os.listdir(foldername) if jsonfile.endswith('.json')]


def get_jsons(jsonfiles, foldername):
    for jsonfile in jsonfiles:
        with open(os.path.join(DATALOC, foldername, jsonfile), 'r') as f:
            for line in f:
                yield json.loads(line)


def get_article_text(html):
    soup = BeautifulSoup(html)
    article = soup.find('article')
    try: 
        h1 = article.find('h1').text 
    except AttributeError: 
        h1 = ''
    try: 
        h2 = article.find('h2').text 
    except AttributeError: 
        h2 = ''
    segments = article.findAll('p')
    fulltext = []
    for segment in segments:
        fulltext.append(segment.text)
    try:
        timetag = soup.find('time')
        timetag = (timetag.text, timetag.attrs['datetime'])
    except AttributeError:
        timetag = ('', '')
    return h1, h2, ' '.join(fulltext[:-1]), timetag


def refugee_related(text):
    for keyword in KEYWORDS:
        if keyword in text:
            return True
    return False


def get_MongoDB_client(connection_string):
    '''Connect to MondoDB.'''
    print('Connecting to: {}.'.format(connection_string))
    client = MongoClient(connection_string, maxPoolSize=50)
    return client


def connect_newsarticlesdb(client):
    '''Connect to, or create, database.'''
    db = client.newsarticles
    return db

def insert_many_newsarticles(jsons, db):
    articles, narticles, skipped, relevant = [], 0, 0, 0
    for article in jsons:
        narticles += 1
        try:
            h1, h2, text, timetag = get_article_text(article['html'])
            article['h1'] = h1
            article['h2'] = h2
            article['body_text'] = text
            article['datetime'] = timetag[1]
            article['publication_day'] = timetag[0]
            related = refugee_related(text)
            relevant.append(related)
            article['refugee_related'] = related
            articles.append(article)
        except AttributeError:
            skipped += 1
        if len(articles) == 1000:
            collection = db.newsarticles
            print('Inserting data into MongoDB. Inserted: {}'.format(narticles))
            collection.insert_many(articles)
            articles = []
    if len(articles) > 0:
        collection.insert_many(articles)
    print('Finished insert of articles. Inserted total: {}. Skipped {}.  Relevant {}.'.format(
        narticles, skipped, sum(relevant))
    )


def main():

    cluster = 'bubblegum-odoit.mongodb.net'

    parser = argparse.ArgumentParser()
    parser.add_argument(
        'publication',
        type=str,
        nargs=1,
        help='Provide name of folder / publication',
    )
    args = parser.parse_args()

    username = input("Username: ")
    pw = getpass.getpass()

    connection_string = 'mongodb+srv://{}:{}@{}/test?retryWrites=true'.format(
        username,
        pw,
        cluster
    )

    client = get_MongoDB_client(connection_string) 
    db = connect_newsarticlesdb(client)

    jsonfiles = get_jsonfiles(args.publication[0])
    jsons = get_jsons(jsonfiles, args.publication[0])

    insert_many_newsarticles(jsons, db)


if __name__=='__main__':
    main()

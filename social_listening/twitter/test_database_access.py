# script for testing database access

from pymongo import MongoClient
import sys

def connect_mongoDB(username):
    '''Connect to MondoDB. Needs pw file (at same location as this script).'''
    with open('mongodb.pw', 'r') as f:
        pw = f.readline().rstrip()
    connection_string = 'mongodb+srv://{}:{}@bananamania-aojj2.mongodb.net/test?retryWrites=true'.format(username,pw)
    client = MongoClient(connection_string, maxPoolSize=50)
    db = client.tweets
    #serverStatusResult = db.command("serverStatus")
    #pprint(serverStatusResult)
    return client



if __name__ == "__main__":
    
    print("username:")
    username = input()
    
    client = connect_mongoDB(username)
    database = client.tweets
    collection = database.tweets
    
    print("SUCCESS")
    
    test_tweet = collection.find_one()
    print("Sample tweet:\n%s" % test_tweet['full_text'])

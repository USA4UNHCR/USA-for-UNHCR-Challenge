## Import Libraries

from pymongo import MongoClient
import pandas as pd
import numpy as np


##################################################################################
#login credentials
username = 'jenny'
pw = 'awesome!'

#parameters
popular_retweetct = 100 #minimun # of retweet counts to include in filter
batchsize = 1000 #loading mongo data in batches for processing

##################################################################################
#functions

#Connect to Mongo
def connect_mongoDB(username,password):
    '''Connect to MondoDB'''
    connection_string = 'mongodb+srv://{}:{}@bananamania-aojj2.mongodb.net/test?retryWrites=true'.format(username,password)
    client = MongoClient(connection_string)
    return client
    
    
#Flatten json Dictionary and convert to dataframe
def flattenDict(d, result=None):
    '''
    Creates pandas dataframe from nested dictionary
    '''
    if result is None:
        result = {}
    for key in d:
        value = d[key]
        if isinstance(value, dict):
            value1 = {}
            for keyIn in value:
                value1[".".join([key,keyIn])]=value[keyIn]
            flattenDict(value1, result)
        elif isinstance(value, (list, tuple)):   
            for indexB, element in enumerate(value):
                if isinstance(element, dict):
                    value1 = {}
                    for keyIn in element:
                        newkey = ".".join([key,keyIn])        
                        value1[".".join([key,keyIn])]=value[indexB][keyIn]
                    for keyA in value1:
                        flattenDict(value1, result)   
        else:
            result[key]=value
    return result

##################################################################################
#Connect to Mongo
client = connect_mongoDB(username,pw)
db = client.tweets

##################################################################################
#Get total tweets with retweets
numdocs = db['tweets'].find({'retweet_count':{'$gte':popular_retweetct},'retweeted_status':{'$exists':True}}).count()

##################################################################################
#Iterate through tweets and drop duplicates leaving only the record with the highest retweet count
popular_tweets=pd.DataFrame()
for i in range(0, numdocs, batchsize):
    templist =  list(db['tweets'].find({'retweet_count':{'$gte':popular_retweetct},'retweeted_status':{'$exists':True}}, {'u4u_dataset':1,'retweeted_status.created_at':1,'retweeted_status.id':1,'retweeted_status.full_text':1,'retweeted_status.favorite_count':1,'retweeted_status.retweet_count':1,'user.description':1,
                                                           'retweeted_status.user.created_at':1,'retweeted_status.user.followers_count':1,'retweeted_status.user.friends_count':1,'retweeted_status.user.lang':1,'retweeted_status.user.listed_count':1,'retweeted_status.user.location':1,
                                                          'retweeted_status.user.name':1,'retweeted_status.user.screen_name':1},skip=i,limit=batchsize))
    tweetdf = pd.DataFrame([flattenDict(tweet) for tweet in templist])
    filtered_df = tweetdf.groupby('retweeted_status.id', group_keys=False).apply(lambda x: x.loc[x["retweeted_status.retweet_count"].idxmax()])
    popular_tweets = popular_tweets.append(filtered_df)
    print('completed ', str(i), ' records')


##################################################################################
#Final de-duping stage

popular_tweets = popular_tweets.drop(['retweeted_status.id'], axis=1).reset_index()
popular_tweets = popular_tweets.groupby('retweeted_status.id', group_keys=False).apply(lambda x: x.loc[x["retweeted_status.retweet_count"].idxmax()])
popular_tweets.to_csv('popular_tweets.csv',index=False, encoding='utf-8-sig')


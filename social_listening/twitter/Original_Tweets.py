#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from pymongo import MongoClient
import pandas as pd
import numpy as np


# In[ ]:


# Insert your own credentials here!
# each one should be a string, between quotes
username = 'jenny'
pw = 'awesome!'


# In[ ]:


def connect_mongoDB(username,password):
    '''Connect to MondoDB'''
    connection_string = 'mongodb+srv://{}:{}@bananamania-aojj2.mongodb.net/test?retryWrites=true'.format(username,password)
    client = MongoClient(connection_string)
    return client


# In[ ]:


client = connect_mongoDB(username,pw)
db = client.tweets

# The queries below are based on MongoDB queries
# The docs are here: https://docs.mongodb.com/manual/reference/operator/query/
numdocs = db['tweets'].find({"retweeted_status":{"$exists":False}}).count()


# In[ ]:


docs = list(db['tweets'].find({"retweeted_status":{"$exists":False}}, {'created_at':1,'full_text':1,'favorite_count':1,'retweet_count':1,'entities.media.type':1,'user.description':1,
                                                           'user.created_at':1,'user.followers_count':1,'user.friends_count':1,'user.lang':1,'user.listed_count':1,'user.location':1,
                                                          'user.name':1,'user.screen_name':1}))


# In[ ]:


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


# In[ ]:


tweetdf = pd.DataFrame([flattenDict(tweet) for tweet in docs])


# In[ ]:


# Enconding 'utf-8-sig' keeps non-Latin characters and other Unicode/Emoji symbols
tweetdf.to_csv('original_tweets.csv',index=False,encoding='utf-8-sig')


#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Analyse reddit posts"""


import re
from afinn import Afinn
import numpy as np  
from nltk.corpus import stopwords
import nltk
from collections import Counter
import json
import logging
import collections
from langdetect import detect
import pandas as pd
import operator


class TwitterAnalyser(object):

    def __init__(self, tweets, currency_symbols, stopwords):

        # logging.basicConfig(filename='reddit_analyser.log',level=logging.DEBUG,
        # format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s', datefmt="%Y-%m-%d %H:%M:%S")
        self.afinn = Afinn()
        self.stopwords = stopwords
        self.tweets = self.CleanseData(tweets)
        self.currency_symbols = currency_symbols

    def TotalTweets(self):
        total_tweets = len(self.tweets.index)
        return total_tweets

    def SentimentByCurrency(self, oldsbc):
        tweet_sentiment = self.tweets_sentiment.copy()
        sbc = self.currency_symbols.copy()

        sbc['Symbol'] = sbc.Symbol.str.lower()
        sbc['Name'] = sbc.Name.str.lower()
        sbc["Sentiment"] = 0

        for symbol in sbc['Symbol']:
            for i, tweet in tweet_sentiment.iterrows():
                if symbol in tweet["Text"]:
                    
                    sbc.loc[sbc['Symbol'] == symbol, 'Sentiment'] = tweet["SA"]


        for name in sbc['Name']:
            for i, tweet in tweet_sentiment.iterrows():
                if name in tweet["Text"]:
                    oldcount = sbc.loc[sbc["Name"] == name, 'Sentiment'].values
                    if len(oldcount) > 0:
                        sbc.loc[sbc['Symbol'] == symbol, 'Sentiment'] = tweet["SA"] + oldcount[0]
        
        sbc = sbc.to_json(orient='records', date_format=None)            
        sbc = json.loads(sbc)

        if oldsbc != None:
 
            updatedsbc = []
            # Compare new users against users already in database 
            # and update old users with new info, popping from array
            # once processed
            for currency in range(len(oldsbc)-1):
                for newcurrency in range(len(sbc)-1):
                    if oldsbc[currency]['Name'] == sbc[newcurrency]['Name']:
                        updatedcurrency = {}
                        updatedcurrency["Name"] = oldsbc[currency]["Name"]
                        updatedcurrency["Symbol"] = oldsbc[currency]["Symbol"]
                        updatedcurrency["Currency"] = oldsbc[currency]["Currency"]
                        updatedcurrency["Sentiment"] = oldsbc[currency]["Sentiment"] + sbc[newcurrency]["Sentiment"]
                        updatedsbc.append(updatedcurrency)
                        sbc.pop(newcurrency)

            # Append all newly found users to updatedous list
            for newcurrency in sbc:
                updatedsbc.append(newcurrency)

            j = json.dumps(updatedsbc)
            return json.loads(j)
        
        return sbc


    def SentimentByDay(self, oldsa):
        ts = self.tweets.copy()
        ts['SA'] = np.array([ self.AnalyseSentiment(text) for text in ts['Text'] ])
        self.tweets_sentiment = ts     
        ts = ts.drop(['Author', 'Text'], 1)
        ts =  ts.groupby('Date')['SA'].sum().reset_index()
        ts['SA'] = ts['SA'] + oldsa

     
        ts = ts.to_json(orient='records', date_format=None)    
        
        return json.loads(ts)
    
    def MostActiveUsers(self, oldmaut):
        maut = self.tweets.copy()
        maut = maut['Author'].value_counts().reset_index().rename(
            columns={'index': 'Author', 'Author': 'n'})

        maut = maut.sort_values('n', ascending=False).head(500)
        maut.fillna(0, inplace=True)
        maut = maut.to_json(orient='records', date_format=None)
        maut = json.loads(maut)

        if oldmaut != None:
            updatedmaut = []
            # Compare new users against users already in database 
            # and update old users with new info, popping from array
            # once processed
            for user in range(len(oldmaut)-1):
                for newuser in range(len(maut)-1):
                    if oldmaut[user]['Author'] == maut[newuser]['Author']:
                        updateduser = {}
                        updateduser["Author"] = oldmaut[user]["Author"]
                        updateduser["n"] = oldmaut[user]["n"] + maut[newuser]["n"]

                        updatedmaut.append(updateduser)
                        maut.pop(newuser)

            # Append all newly found users to updatedmaut list
            for newuser in maut:
                updatedmaut.append(newuser)

            j = json.dumps(updatedmaut)
            return json.loads(j)
        
        return maut
    
    def Bigram(self, oldb):
        """
        Creating a list of bigram frequencies from the gathered 
        reddit comments and posts
        """
        tweets = self.tweets.copy()
        tweets_counts = collections.Counter()
        for sent in tweets["Text"]:
            words = nltk.word_tokenize(sent)
            tweets_counts.update(nltk.bigrams(words))

        bigram = pd.DataFrame(list(tweets_counts.items()), columns=['bigram', 'n'])
        bigram.fillna(0, inplace=True)

        bigram["bigram"] = bigram["bigram"].apply(lambda x: ' '.join(x))
        self.bigram = bigram
        bigram = bigram.to_json(orient='records', date_format=None)

        if oldb != None:
            
            bigram = json.loads(bigram)
            updatedbigrams = []

            for oldbigram in range(len(oldb)-1):
                oldelement = oldb[oldbigram]
                for newbigram in range(len(bigram)-1):
                    newelement = bigram[newbigram]
                    if oldelement["bigram"] == newelement["bigram"]:
                        
                        updated = {}
                        updated["bigram"] = oldelement["bigram"]
                        updated["n"] = oldelement["n"] + newelement["n"]

                        updatedbigrams.append(updated)
                        bigram.pop(newbigram)
            
            # Append all newly found users to updatedous list
            for b in bigram:
                updatedbigrams.append(b)

            j = json.dumps(updatedbigrams)
            return json.loads(j)

        return json.loads(bigram)
    
    def BigramByDay(self, oldbigrams):
        bigramscopy = self.bigram.copy()
        bigrams = bigramscopy.sort_values('n',ascending = False).head(25)
        bigrams = bigrams.to_json(orient='records', date_format=None)
        bigrams = json.loads(bigrams)

        if oldbigrams != None:
            updatedbigrams =[]
            for old in range(len(oldbigrams)-1):
                oldelement = oldbigrams[old]
                for new in range(len(bigrams)-1):
                    newelement = bigrams[new]
                    if oldelement["bigram"] == newelement["bigram"]:
                        
                        updated = {}
                        updated["bigram"] = oldelement["bigram"]
                        updated["n"] = oldelement["n"] + newelement["n"]

                        updatedbigrams.append(updated)
                        bigrams.pop(new)

            # Append all newly found users to updatedous list
            for b in bigrams:
                updatedbigrams.append(b)

            j = json.dumps(updatedbigrams[:25])
            return json.loads(j)
            
        return bigrams


    def WordCount(self, oldwc):
        wc = self.tweets.copy()
        wc = list(collections.Counter(" ".join(wc['Text']).split()).items())
        wc = pd.DataFrame(wc, columns=['word', 'n'])
        
        # wc.fillna(0, inplace=True)                
        self.word_count = wc
        wc = wc.to_json(orient='records', date_format=None) 

        if oldwc != None:
            wc = json.loads(wc)
            updatedwc = []
            # Compare new users against users already in database 
            # and update old users with new info, popping from array
            # once processed
            for word in range(len(oldwc)-1):
                for newword in range(len(wc)-1):
                    if oldwc[word]['word'] == wc[newword]['word']:
                        updatedword = {}
                        updatedword["word"] = oldwc[word]["word"]
                        updatedword["n"] = oldwc[word]["n"] + wc[newword]["n"]
                        updatedwc.append(updatedword)
                        wc.pop(newword)

            # Append all newly found users to updatedous list
            for newword in wc:
                updatedwc.append(newword)

            j = json.dumps(updatedwc)
            return json.loads(j)

        return json.loads(wc)
    
    def WordCountByDay(self, oldwordcount):
        tweets_copy = self.tweets.copy()        
        # Change date format from timestamp
        tweets_copy["Date"] = pd.to_datetime(tweets_copy['Date'], errors='coerce')
        # Convert back to string date without hours
        tweets_copy["Date"] = tweets_copy["Date"].dt.strftime('%Y-%m-%d')
        # Group by date
        tweets_copy = tweets_copy.groupby('Date')
        # Loop through dataframe counting most common 25 words for each date
        wordcount= []
        for name, group in tweets_copy:
            texts = " ".join(group['Text'])
            groupCounts = Counter(texts.split()).most_common(25)
            wordcount.append([name, dict(groupCounts)])

        # Create dataframe with counts
        wordcount = pd.DataFrame(wordcount, columns = ['Date','counts'])
        wordcount = wordcount.to_json(orient='records', date_format=None)
        wordcount = json.loads(wordcount)

        if oldwordcount != None:
            updatedwordcount = oldwordcount
            temp_word_counts = list(wordcount[0]['counts'].items())

                  
            for old_item in list(updatedwordcount.items()):
                for new_item in temp_word_counts:
                    if old_item[0] == new_item[0]:
                        updatedwordcount[old_item[0]] = old_item[1] + new_item[1]
                        temp_word_counts.remove(new_item)
                                            
            # Append all newly found users to updatedous list
            for item in temp_word_counts:
                updatedwordcount[item[0]] = item[1]
                temp_word_counts.remove(item)
            
            sort = sorted(updatedwordcount.items(), key=operator.itemgetter(1), reverse=True)   
            j = json.dumps(dict(sort[:25]))
           
            return json.loads(j)
   
        return wordcount





        # wordcount = wordcountcopy.sort_values('n',ascending = False).head(25)
        # wordcount = wordcount.to_json(orient='records', date_format=None)
        # wordcount = json.loads(wordcount)

        # if oldwordcount != None:
        #     updatedwordcount =[]
        #     for old in range(len(oldwordcount)-1):
        #         oldelement = oldwordcount[old]
        #         for new in range(len(wordcount)-1):
        #             newelement = wordcount[new]
        #             if oldelement["word"] == newelement["word"]:
                        
        #                 updated = {}
        #                 updated["word"] = oldelement["word"]
        #                 updated["n"] = oldelement["n"] + newelement["n"]
        #                 updatedwordcount.append(updated)
        #                 wordcount.pop(new)

        #     # Append all newly found users to updatedous list
        #     for b in wordcount:
        #         updatedwordcount.append(b)

        #     j = json.dumps(updatedwordcount[:25])
        #     return json.loads(j)
            
        # return wordcount

    def CurrencyMentions(self, oldcm):

        word_count = self.word_count
        cm = self.currency_symbols.copy()

        cm['Symbol'] = cm.Symbol.str.lower()
        cm['Name'] = cm.Name.str.lower()
        cm["Mentions_Sym"] = 0
        cm["Mentions_Name"] = 0

        for symbol in cm['Symbol']:
            c = word_count.loc[word_count['word'] == symbol, 'n'].values
            if len(c) > 0:
                cm.loc[cm['Symbol'] == symbol, 'Mentions_Sym'] = c[0]

        for name in cm['Name']:
            c = word_count.loc[word_count['word'] == name, 'n'].values
            if len(c) > 0:
                cm.loc[cm['Name'] == name, 'Mentions_Name'] = c[0]
        
        self.currency_mentions = cm

        cm = cm.to_json(orient='records', date_format=None)

        if oldcm != None:
            cm = json.loads(cm)
            updatedcm = []
            # Compare new users against users already in database 
            # and update old users with new info, popping from array
            # once processed
            for currency in range(len(oldcm)-1):
                for newcurrency in range(len(cm)-1):
                    if oldcm[currency]['Name'] == cm[newcurrency]['Name']:
                        updatedcurrency = {}
                        updatedcurrency["Name"] = oldcm[currency]["Name"]
                        updatedcurrency["Symbol"] = oldcm[currency]["Symbol"]
                        updatedcurrency["Mentions_Name"] = oldcm[currency]["Mentions_Name"] + cm[newcurrency]["Mentions_Name"]
                        updatedcurrency["Mentions_Sym"] = oldcm[currency]["Mentions_Sym"] + cm[newcurrency]["Mentions_Sym"]
                        updatedcm.append(updatedcurrency)
                        cm.pop(newcurrency)

            # Append all newly found users to updatedous list
            for newcurrency in cm:
                updatedcm.append(newcurrency)

            j = json.dumps(updatedcm)
            return json.loads(j)
        return json.loads(cm)

    def CurrencyMentionsByDay(self, oldcmbd):
        cmcopy = self.currency_mentions.copy()
        cmbd = cmcopy.to_json(orient='records', date_format=None)
        cmbd = json.loads(cmbd)

        if oldcmbd != None:
            updatedcmbd =[]
            for old in range(len(oldcmbd)-1):
                oldelement = oldcmbd[old]
                for new in range(len(cmbd)-1):
                    newelement = cmbd[new]
                    if oldelement['Name'] == newelement['Name']:
                        updatedcurrency = {}
                        updatedcurrency["Name"] = oldelement["Name"]
                        updatedcurrency["Symbol"] = oldelement["Symbol"]
                        updatedcurrency["Mentions_Name"] = oldelement["Mentions_Name"] + newelement["Mentions_Name"]
                        updatedcurrency["Mentions_Sym"] = oldelement["Mentions_Sym"] + newelement["Mentions_Sym"]
                        updatedcmbd.append(updatedcurrency)
                        cmbd.pop(new)

            # Append all newly found users to updatedous list
            for b in cmbd:
                updatedcmbd.append(b)

            j = json.dumps(updatedcmbd)
            return json.loads(j)
            
        return cmbd



    def CleanText(self, text):
        return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", str(text)).split())

    def AnalyseSentiment(self, text):
        analysis = self.CleanText(text)
        return self.afinn.score(analysis)

    def CleanseData(self, source):
        
        #Change date format
        source["Date"] = pd.to_datetime(source["Date"],unit='s')
        #Create df object
        data = {"Author": source["Author"], "Text": source["Text"], "Date": source["Date"]}
        #Create df
        data = pd.DataFrame(data=data)
        # #Convert datetime to date
        data["Date"] = data["Date"].dt.strftime('%Y-%m-%d %H:00:00')
        #Remove URLs  
        data["Text"] =  data['Text'].str.replace(r'http\S+', '', case=False)
        #Remove Na's
        data = data.dropna(how='any',axis=0)
        #Remove punctuation
        data["Text"] = data["Text"].str.replace('[^\w\s]','')
        #To lower case
        data['Text'] = data.Text.str.lower()
        #Remove Stop words and no english words
        stop = self.stopwords['word'].tolist()   
        data["Text"] = data["Text"].apply(lambda x: ' '.join([word for word in x.split() if word not in stop ]))
        try:
            data = data[data["Text"].apply(lambda x: detect(x) == 'en')]
        except:
            print("No language detected")
        
        return data

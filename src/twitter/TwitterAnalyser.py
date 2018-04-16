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
        ts = ts.groupby('Date')['SA'].sum().reset_index()

        if oldsa != None:
            oldsa = pd.DataFrame.from_records(data=oldsa)
            oldnew_merged = pd.concat([ts,oldsa])
            oldnew_merged = oldnew_merged.groupby('Date').sum().reset_index()
            oldnew_merged = oldnew_merged.to_json(orient='records', date_format=None)
            oldnew_merged= json.loads(oldnew_merged)
            return oldnew_merged
     
        ts = ts.to_json(orient='records', date_format=None)    
        return json.loads(ts)
    
    def MostActiveUsers(self, oldmaut):
        maut = self.tweets.copy()
        maut = maut['Author'].value_counts().reset_index().rename(
            columns={'index': 'Author', 'Author': 'n'})

        maut = maut.sort_values('n', ascending=False).head(500)
        maut.fillna(0, inplace=True)

        if oldmaut != None:
            oldmaut = pd.DataFrame.from_records(data=oldmaut)
            oldnew_merged = pd.concat([maut,oldmaut])
            oldnew_merged = oldnew_merged.groupby('Author').sum().reset_index()
            oldnew_merged = oldnew_merged.sort_values('n', ascending=False).head(500)
            oldnew_merged = oldnew_merged.to_json(orient='records', date_format=None)
            oldnew_merged= json.loads(oldnew_merged)
            return oldnew_merged

        maut = maut.to_json(orient='records', date_format=None)
        maut = json.loads(maut)

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
        bigram = bigram.head(500)
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
        tweets_copy = self.tweets.copy()        
        # Change date format from timestamp
        tweets_copy["Date"] = pd.to_datetime(tweets_copy['Date'], errors='coerce')
        # Convert back to string date without hours
        tweets_copy["Date"] = tweets_copy["Date"].dt.strftime('%Y-%m-%d')
        # Group by date
        tweets_copy = tweets_copy.groupby('Date')
        # Loop through dataframe counting most common 25 words for each date
        bigrams = []

         # loop through groups
        for name, group in tweets_copy:
            counts = collections.Counter()
            # Loop through text counting bigrams
            for sent in group["Text"]:
                words = nltk.word_tokenize(sent)
                counts.update(nltk.bigrams(words))
            updated = {}
            # join bigrams to one word
            for key, value in counts.most_common(25):
                k = ' '.join(key)
                updated[k] = value

            # append grouped date and counted bigrams to list
            bigrams.append([name, updated])

        
        # Create dataframe with counts
        bbd = pd.DataFrame(bigrams, columns = ['Date','counts'])


        if oldbigrams != None:
            oldbigrams = pd.DataFrame.from_records(data=oldbigrams)
            oldnew_merged = pd.concat([bbd,oldbigrams])

            oldnew_merged = oldnew_merged.groupby('Date')
            flattened = []
            # loop through groups
            for name, group in oldnew_merged:
                for obj in list(group["counts"]):
                    for item in obj.items():
                        flattened.append([name, item[0], item[1]])

            # Goal is to group similar dates and marge words together 
            # keeping the 25 most popular for each date
            flattened = pd.DataFrame(flattened, columns = ['Date','words','n'])
            flattened = flattened.groupby(['Date', 'words'])['n'].sum().reset_index()
            flattened = flattened.sort_values(['Date','n'], ascending=[True, False])
            flattened = flattened.groupby('Date', as_index=False).head(25)
            transformed = []
            # loop through groups
            for name, group in flattened.groupby('Date'):
                group_dict = dict(zip(group.words, group.n))
                transformed.append([name, group_dict])
            
            transformed = pd.DataFrame(transformed, columns = ['Date','counts'])
            transformed = transformed.to_json(orient='records', date_format=None)
            transformed= json.loads(transformed)
            return transformed

        bbd = bbd.to_json(orient='records', date_format=None)
        bbd = json.loads(bbd)
            
        return bbd


    def WordCount(self, oldwc):
        wc = self.tweets.copy()
        wc = list(collections.Counter(" ".join(wc['Text']).split()).items())
        wc = pd.DataFrame(wc, columns=['word', 'n'])
        
        # wc.fillna(0, inplace=True)                
        self.word_count = wc
        wc = wc.head(500)        
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

        if oldwordcount != None:
            oldwordcount = pd.DataFrame.from_records(data=oldwordcount)
            oldnew_merged = pd.concat([wordcount,oldwordcount])
            oldnew_merged = oldnew_merged.groupby('Date')
            flattened = []
            # loop through groups
            for name, group in oldnew_merged:
                for obj in list(group["counts"]):
                    for item in obj.items():
                        flattened.append([name, item[0], item[1]])

            # Goal is to group similar dates and marge words together 
            # keeping the 25 most popular for each date
            flattened = pd.DataFrame(flattened, columns = ['Date','words','n'])
            flattened = flattened.groupby(['Date', 'words'])['n'].sum().reset_index()
            flattened = flattened.sort_values(['Date','n'], ascending=[True, False])
            flattened = flattened.groupby('Date', as_index=False).head(25)
            transformed = []
            # loop through groups
            for name, group in flattened.groupby('Date'):
                group_dict = dict(zip(group.words, group.n))
                transformed.append([name, group_dict])
            
            transformed = pd.DataFrame(transformed, columns = ['Date','counts'])
            transformed = transformed.to_json(orient='records', date_format=None)
            transformed= json.loads(transformed)
            return transformed

        wordcount = wordcount.to_json(orient='records', date_format=None)
        wordcount = json.loads(wordcount)

        return wordcount


    def CurrencyMentions(self, oldcm):

        word_count = self.word_count
        bigram = self.bigram        
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
            if len(name.split()) == 1:
                c = word_count.loc[word_count['word'] == name, 'n'].values
                if len(c) > 0:
                    cm.loc[cm['Name'] == name, 'Mentions_Name'] = c[0]
            else:
                c = bigram.loc[bigram['bigram'] == name, 'n'].values
                if len(c) > 0:
                    cm.loc[cm['Name'] == name, 'Mentions_Name'] = c[0]        

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
        tweets = self.tweets.copy()
        # Change date format from timestamp
        tweets["Date"] = pd.to_datetime(tweets['Date'], errors='coerce')
        # Convert back to string date without hours
        tweets["Date"] = tweets["Date"].dt.strftime('%Y-%m-%d %H:00:00')  
        # Group by date
        tweets = tweets.groupby('Date')
        bigram = []
        word_count = []        
        # loop through groups
        for name, group in tweets:
            # word count
            texts = " ".join(group['Text'])
            word_count.append([name, dict(Counter(texts.split()))])
            ####
            # Bigrams
            merged_counts = collections.Counter()
            # Loop through text counting bigrams
            for sent in group["Text"]:
                words = nltk.word_tokenize(sent)
                merged_counts.update(nltk.bigrams(words))
            updated_full = {}
            # join full list of bigrams to one word
            for key, value in merged_counts.items():
                k = ' '.join(key)
                updated_full[k] = value
            # append grouped date and counted bigrams to list
            bigram.append([name, updated_full])
        # Create dataframe with counts
        bigram = pd.DataFrame(bigram, columns = ['Date','counts'])
        # Create dataframe with counts
        word_count = pd.DataFrame(word_count, columns = ['Date','counts'])

        # Create merged dataframe
        merged = {"Date": word_count["Date"], "word_count": word_count["counts"], "bigram_count": bigram["counts"]}
        merged = pd.DataFrame(data=merged)
        # Group by date
        merged = merged.groupby('Date')
        
        cm = self.currency_symbols.copy()
        cm['Symbol'] = cm.Symbol.str.lower()
        cm['Name'] = cm.Name.str.lower()
        cm = cm.drop('Currency', 1)
        cm["Mentions_Sym"] = 0
        cm["Mentions_Name"] = 0

        cmbd = [] 
        for date, group in merged:
            word_count = group["word_count"].tolist()
            bigram_count = group["bigram_count"].tolist()
            temp_cm = cm
            for symbol in temp_cm['Symbol']:
                if symbol in word_count[0]:
                    temp_cm.loc[temp_cm['Symbol'] == symbol, 'Mentions_Sym'] = word_count[0][symbol]
            for name in temp_cm['Name']:
                if len(name.split()) == 1:
                    if name in word_count[0]:
                        temp_cm.loc[temp_cm['Name'] == name, 'Mentions_Name'] = word_count[0][name]
                else:
                    if name in bigram_count[0]:
                        temp_cm.loc[temp_cm['Name'] == name, 'Mentions_Name'] = bigram_count[0][name]

            temp_cm["n"] = temp_cm["Mentions_Name"] + temp_cm["Mentions_Sym"]
            temp_cm = temp_cm.drop(['Mentions_Name','Mentions_Sym'], 1)
            temp_cm = temp_cm[temp_cm['n'] != 0]
            temp_cm = temp_cm.to_json(orient='records', date_format=None)
            temp_cm = json.loads(temp_cm)
            cmbd.append([date, temp_cm])
                
        
        cmbd = pd.DataFrame(cmbd, columns = ['Date','counts'])

        if oldcmbd != None:
            oldcmbd = pd.DataFrame.from_records(data=oldcmbd)
            oldnew_merged = pd.concat([cmbd,oldcmbd])
            oldnew_merged = oldnew_merged.groupby('Date')
            flattened = []
            # loop through groups
            for name, group in oldnew_merged:
                for obj in list(group["counts"]):
                    for item in obj:
                        flattened.append([name, item['Symbol'], item['Name'],item['n']])

            

            flattened = pd.DataFrame(flattened, columns = ['Date','Symbol','Name', 'n'])
            flattened = flattened.groupby(['Date','Symbol','Name'])['n'].sum().reset_index()
            transformed = []

            # loop through groups
            for name, group in flattened.groupby('Date'):
                objs = [{'Symbol':a, 'Name':b, 'n':c} for a,b,c in zip(group.Symbol, group.Name, group.n)]
                transformed.append([name, objs])
            
            transformed = pd.DataFrame(transformed, columns = ['Date','counts'])
            transformed = transformed.to_json(orient='records', date_format=None)
            transformed= json.loads(transformed)
            return transformed

        cmbd = cmbd.to_json(orient='records', date_format=None)
        cmbd = json.loads(cmbd)
            
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
        # Remove Hashtags
        data["Text"] = data["Text"].str.replace(r"#(\w+)",'')
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

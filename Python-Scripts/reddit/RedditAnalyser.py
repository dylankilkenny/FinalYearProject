#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Analyse reddit posts"""

import pandas as pd
import re
from afinn import Afinn
import numpy as np  
from nltk.corpus import stopwords
import nltk
from collections import Counter
import json
import logging

class RedditAnalyser(object):

    def __init__(self, comments, posts, currency_symbols, stopwords):

        logging.basicConfig(filename='reddit_analyser.log',level=logging.DEBUG,
        format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s', datefmt="%Y-%m-%d %H:%M:%S")
        self.afinn = Afinn()
        self.stopwords = stopwords
        self.comments = self.CleanseData(comments, False)
        self.posts = self.CleanseData(posts, True)
        self.currency_symbols = currency_symbols


    def NoPostComments(self):
        nc = len(self.comments.index)
        np = len(self.posts.index)
        return (nc, np)

    
    def CommentsPostsByDay(self, oldc, oldp):
        cbd = self.comments.copy()
        cbd = cbd.groupby(['Date']).size().to_frame('n_comment').reset_index()

        pbd = self.posts.copy()
        pbd = pbd.groupby(['Date']).size().to_frame('n_post').reset_index()

        cpbd = pd.merge(cbd, pbd, on='Date', how='outer')
        print(cpbd)
        cpbd['n_post'] = cpbd['n_post'] + oldp
        cpbd['n_comment'] = cpbd['n_comment'] + oldc
        cpbd = cpbd.to_json(orient='records', date_format=None)

        return json.loads(cpbd)


    
    def MostActiveUsers(self, oldmau):
        mauc = self.comments.copy()
        mauc = mauc['Author'].value_counts().reset_index().rename(
            columns={'index': 'Author', 'Author': 'Comments'})

        maup = self.posts.copy()
        maup = maup['Author'].value_counts().reset_index().rename(
            columns={'index': 'Author', 'Author': 'Posts'}) 

        mau = pd.merge(maup, mauc, on='Author', how='outer')
        mau['Activity'] = mau['Comments'] + mau['Posts']
        mau = mau.sort_values('Activity', ascending=False).head(100)
        mau.fillna(0, inplace=True)
        mau = mau.to_json(orient='records', date_format=None)

        if oldmau != None:
            mau = json.loads(mau)
            updatedmau = []
            # Compare new users against users already in database 
            # and update old users with new info, popping from array
            # once processed
            for user in range(len(oldmau)-1):
                for newuser in range(len(mau)-1):
                    if oldmau[user]['Author'] == mau[newuser]['Author']:
                        updateduser = {}
                        updateduser["Author"] = oldmau[user]["Author"]
                        updateduser["Comments"] = oldmau[user]["Comments"] + mau[newuser]["Comments"]
                        updateduser["Posts"] = oldmau[user]["Posts"] + mau[newuser]["Posts"]
                        updateduser["Activity"] = oldmau[user]["Activity"] + mau[newuser]["Activity"]
                        updatedmau.append(updateduser)
                        mau.pop(newuser)

            # Append all newly found users to updatedmau list
            for newuser in mau:
                updatedmau.append(newuser)

            j = json.dumps(updatedmau)
            return json.loads(j)
        
        return json.loads(mau)

    def OverallUserScore(self, oldous):
        ousc = self.comments.copy()
        ousc =  ousc.groupby('Author')['Score'].sum().reset_index().rename(
            columns={'Author': 'Author', 'Score': 'Comments'})

        ousp = self.posts.copy()
        ousp =  ousp.groupby('Author')['Score'].sum().reset_index().rename(
            columns={'Author': 'Author', 'Score': 'Posts'})

        ous = pd.merge(ousc, ousp, on='Author', how='outer').sort_values('Comments', ascending=False)
        ous['TotalScore'] = ous['Comments'] + ous['Posts']
        ous = ous.sort_values('TotalScore', ascending=False).head(100)
        ous.fillna(0, inplace=True)        
        ous = ous.to_json(orient='records', date_format=None)

        if oldous != None:
            print("entered")
            
            ous = json.loads(ous)
            updatedous = []
            # Compare new users against users already in database 
            # and update old users with new info, popping from array
            # once processed
            for user in range(len(oldous)-1):
                for newuser in range(len(ous)-1):
                    if oldous[user]['Author'] == ous[newuser]['Author']:
                        updateduser = {}
                        updateduser["Author"] = oldous[user]["Author"]
                        updateduser["Comments"] = oldous[user]["Comments"] + ous[newuser]["Comments"]
                        updateduser["Posts"] = oldous[user]["Posts"] + ous[newuser]["Posts"]
                        updateduser["TotalScore"] = oldous[user]["TotalScore"] + ous[newuser]["TotalScore"]
                        updatedous.append(updateduser)
                        ous.pop(newuser)

            # Append all newly found users to updatedous list
            for newuser in ous:
                updatedous.append(newuser)

            j = json.dumps(updatedous)
            return json.loads(j)

        return json.loads(ous)       


    def SentimentByDay(self, oldpsa, oldcsa, olds):
        cs = self.comments.copy()
        cs['Comment_SA'] = np.array([ self.AnalyseSentiment(text) for text in cs['Text'] ])
        cs = cs.drop(['Author', 'Score', 'Text'], 1)
        cs =  cs.groupby('Date')['Comment_SA'].sum().reset_index()

        ps = self.posts.copy()
        ps['Post_SA'] = np.array([ self.AnalyseSentiment(text) for text in ps['Text'] ])
        ps = ps.drop(['Author', 'Score', 'Text'], 1)
        ps =  ps.groupby('Date')['Post_SA'].sum().reset_index()

        sbd = pd.merge(cs, ps, on='Date', how='outer')
        sbd["Sentiment"] = sbd["Comment_SA"] + sbd["Post_SA"]

        sbd['Post_SA'] = sbd['Post_SA'] + oldpsa
        sbd['Comment_SA'] = sbd['Comment_SA'] + oldcsa
        sbd['Sentiment'] = sbd['Sentiment'] + olds
        
        sbd = sbd.to_json(orient='records', date_format=None)        
        return json.loads(sbd)

    def WordCount(self):
        wcc = self.comments.copy()
        wcc = Counter(" ".join(wcc['Text']).split()).most_common(300)
        wcc = pd.DataFrame(wcc, columns=['word', 'n_comment'])

        wcp = self.posts.copy()
        wcp = Counter(" ".join(wcp['Text']).split()).most_common(300)
        wcp = pd.DataFrame(wcp, columns=['word', 'n_post'])

        wc = pd.merge(wcc, wcp, on='word', how='outer')
        wc['n'] = wc['n_comment'] + wc['n_post']
        
        self.word_count = wc
        wc = wc.to_json(orient='records', date_format=None) 
        return json.loads(wc)


    def CurrencyMentions(self):
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
        
        cm = cm.to_json(orient='records', date_format=None) 
        return json.loads(cm)
        

            
    def CleanText(self, text):
        return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", str(text)).split())

    
    def AnalyseSentiment(self, text):
        analysis = self.CleanText(text)
        return self.afinn.score(analysis)

    
    def CleanseData(self, source, posts):
        #Change date format
        source["Date"] = pd.to_datetime(source["Date"],unit='s')
        #Create df object
        if posts:
            data = {"Author": source["Author"], "Text": source["Title"], "Date": source["Date"], "Score": source["Score"] }  
        else:
            data = {"Author": source["Author"], "Text": source["Body"], "Date": source["Date"], "Score": source["Score"] }
        #Create df
        data = pd.DataFrame(data=data)
        #Convert datetime to date
        data["Date"] = data["Date"].dt.strftime('%Y-%m-%d')
        #Remove URLs  
        data["Text"] =  data['Text'].str.replace(r'http\S+', '', case=False)
        #Remove Na's
        data = data.dropna(how='any',axis=0)
        #Remove punctuation
        data["Text"] = data["Text"].str.replace('[^\w\s]','')
        #To lower case
        data['Text'] = data.Text.str.lower()
        #Remove Stop words
        stop = self.stopwords['word'].tolist()
        data["Text"] = data["Text"].apply(lambda x: ' '.join([word for word in x.split() if word not in stop]))
        # CommentSentiment
        return data

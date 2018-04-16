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
import collections
from nltk.collocations import *
from pprint import pprint
import operator


class RedditAnalyser(object):

    def __init__(self, comments, posts, currency_symbols, stopwords):

        # logging.basicConfig(filename='reddit_analyser.log',level=logging.DEBUG,
        # format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s', datefmt="%Y-%m-%d %H:%M:%S")
        self.afinn = Afinn()
        self.stopwords = stopwords
        self.number_comments = len(comments.index)
        self.number_posts = len(posts.index)
        self.comments = self.CleanseData(comments, False)
        self.posts = self.CleanseData(posts, True)
        self.currency_symbols = currency_symbols

    def NoPostComments(self):
        nc = self.number_comments
        np = self.number_posts
        return (nc, np)

    def CommentsPostsByDay(self, oldcpbd):
        cbd = self.comments.copy()
        cbd = cbd.groupby(['Date']).size().to_frame('n_comment').reset_index()

        pbd = self.posts.copy()
        pbd = pbd.groupby(['Date']).size().to_frame('n_post').reset_index()

        cpbd = pd.merge(cbd, pbd, on='Date', how='outer')
        cpbd.fillna(0, inplace=True)                
        
        if oldcpbd != None:
            oldcpbd = pd.DataFrame.from_records(data=oldcpbd)
            oldnew_merged = pd.concat([cpbd,oldcpbd])
            oldnew_merged = oldnew_merged.groupby('Date').sum().reset_index()
            oldnew_merged = oldnew_merged.to_json(orient='records', date_format=None)
            oldnew_merged= json.loads(oldnew_merged)
            return oldnew_merged

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

    def SentimentByDay(self, oldsbd):
        cs = self.comments.copy()
        cs['Comment_SA'] = np.array([ self.AnalyseSentiment(text) for text in cs['Text'] ])
        cs = cs.drop(['Author', 'Score', 'Text'], 1)
        cs =  cs.groupby('Date')['Comment_SA'].sum().reset_index()

        ps = self.posts.copy()
        ps['Post_SA'] = np.array([ self.AnalyseSentiment(text) for text in ps['Text'] ])
        ps = ps.drop(['Author', 'Score', 'Text'], 1)
        ps =  ps.groupby('Date')['Post_SA'].sum().reset_index()

        sbd = pd.merge(cs, ps, on='Date', how='outer')
        sbd.fillna(0, inplace=True)                
        sbd["Sentiment"] = sbd["Comment_SA"] + sbd["Post_SA"]

        if oldsbd != None:
            oldsbd = pd.DataFrame.from_records(data=oldsbd)
            oldnew_merged = pd.concat([sbd,oldsbd])
            oldnew_merged = oldnew_merged.groupby('Date').sum().reset_index()
            oldnew_merged = oldnew_merged.to_json(orient='records', date_format=None)
            oldnew_merged= json.loads(oldnew_merged)
            return oldnew_merged

        sbd = sbd.to_json(orient='records', date_format=None)        
        return json.loads(sbd)
    
    def Bigram(self, oldb):
        """
        Creating a list of bigram frequencies from the gathered 
        reddit comments and posts
        """
        comments = self.comments.copy()
        comment_counts = collections.Counter()
        for sent in comments["Text"]:
            words = nltk.word_tokenize(sent)
            comment_counts.update(nltk.bigrams(words))
        bc = pd.DataFrame(list(comment_counts.items()), columns=['bigram', 'n_comment'])

        posts = self.posts.copy()
        post_counts = collections.Counter()
        for sent in posts["Text"]:
            words = nltk.word_tokenize(sent)
            post_counts.update(nltk.bigrams(words))
        bp = pd.DataFrame(list(post_counts.items()), columns=['bigram', 'n_post'])

        bigram = pd.merge(bc, bp, on='bigram', how='outer')
        bigram['n'] = bigram['n_comment'] + bigram['n_post']
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
                        updated["n_post"] = oldelement["n_post"] + newelement["n_post"]
                        updated["n_comment"] = oldelement["n_comment"] + newelement["n_comment"]
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
        # Merge both datasets
        comments = self.comments.copy()
        posts = self.posts.copy()
        # Change date format from timestamp
        comments["Date"] = pd.to_datetime(comments['Date'], errors='coerce')
        posts["Date"] = pd.to_datetime(posts['Date'], errors='coerce')
        # Convert back to string date without hours
        comments["Date"] = comments["Date"].dt.strftime('%Y-%m-%d')
        posts["Date"] = posts["Date"].dt.strftime('%Y-%m-%d')
        merged = pd.concat([posts,comments])   
        # Group by date
        merged = merged.groupby('Date')
        bbd = []
        # loop through groups
        for name, group in merged:
            merged_counts = collections.Counter()
            # Loop through text counting bigrams
            for sent in group["Text"]:
                words = nltk.word_tokenize(sent)
                merged_counts.update(nltk.bigrams(words))
            updated = {}
            # join bigrams to one word
            for key, value in merged_counts.most_common(25):
                k = ' '.join(key)
                updated[k] = value
            # append grouped date and counted bigrams to list
            bbd.append([name, updated])

        # Create dataframe with counts
        bbd = pd.DataFrame(bbd, columns = ['Date','counts'])
        
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
        wcc = self.comments.copy()
        wcc = list(collections.Counter(" ".join(wcc['Text']).split()).items())
        wcc = pd.DataFrame(wcc, columns=['word', 'n_comment'])

        wcp = self.posts.copy()
        wcp = list(collections.Counter(" ".join(wcp['Text']).split()).items())
        wcp = pd.DataFrame(wcp, columns=['word', 'n_post'])

        wc = pd.merge(wcc, wcp, on='word', how='outer')
        wc.fillna(0, inplace=True)
        wc['n'] = wc['n_comment'] + wc['n_post']      
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
                        updatedword["n_comment"] = oldwc[word]["n_comment"] + wc[newword]["n_comment"]
                        updatedword["n_post"] = oldwc[word]["n_post"] + wc[newword]["n_post"]
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
        # Copy posts and comments
        comments_copy = self.comments.copy()
        posts_copy = self.posts.copy()

        # Change date format from timestamp
        comments_copy["Date"] = pd.to_datetime(comments_copy['Date'], errors='coerce')
        posts_copy["Date"] = pd.to_datetime(posts_copy['Date'], errors='coerce')
        # Convert back to string date without hours
        comments_copy["Date"] = comments_copy["Date"].dt.strftime('%Y-%m-%d')
        posts_copy["Date"] = posts_copy["Date"].dt.strftime('%Y-%m-%d')
        # Merge both datasets
        merged = pd.concat([comments_copy,posts_copy])
        # Group by date
        merged = merged.groupby('Date')

        # Loop through dataframe counting most common 25 words for each date
        wcbd= []
    
        for name, group in merged:
            texts = " ".join(group['Text'])
            groupCounts = Counter(texts.split()).most_common(25)
            wcbd.append([name, dict(groupCounts)])

        # Create dataframe with counts
        wcbd = pd.DataFrame(wcbd, columns = ['Date','counts'])


        if oldwordcount != None:
            oldwordcount = pd.DataFrame.from_records(data=oldwordcount)
            oldnew_merged = pd.concat([wcbd,oldwordcount])
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
        
        wcbd = wcbd.to_json(orient='records', date_format=None)
        wcbd = json.loads(wcbd)
            
        return wcbd


    def CurrencyMentions(self, oldcm):
        # Currency mentions single word
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
        
        cm["n"] = cm["Mentions_Name"] + cm["Mentions_Sym"]
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
                        updatedcurrency["Currency"] = oldcm[currency]["Currency"]
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
        
        # Merge both datasets
        comments = self.comments.copy()
        posts = self.posts.copy()
        # Change date format from timestamp
        comments["Date"] = pd.to_datetime(comments['Date'], errors='coerce')
        posts["Date"] = pd.to_datetime(posts['Date'], errors='coerce')
        # Convert back to string date without hours
        comments["Date"] = comments["Date"].dt.strftime('%Y-%m-%d %H:00:00')
        posts["Date"] = posts["Date"].dt.strftime('%Y-%m-%d %H:00:00')
        merged = pd.concat([posts,comments])   
        # Group by date
        merged = merged.groupby('Date')
        bigram = []
        word_count = []        
        # loop through groups
        for name, group in merged:
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
            oldnew_merged =  oldnew_merged[oldnew_merged['counts'].str.len() != 0]
            if len(oldnew_merged) < 1:
                return None
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
        # bit of a hack. removes rows where no counts have been found
        cmbd =  cmbd[cmbd['counts'].str.len() != 0]
        if len(cmbd) < 1:
                return None
        cmbd = cmbd.to_json(orient='records', date_format=None)
        cmbd = json.loads(cmbd)

        return cmbd
         
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
        data["Date"] = data["Date"].dt.strftime('%Y-%m-%d %H:00:00')
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

        with open("../data/banned_users.json", "r") as jsonFile:
            users = json.load(jsonFile)

        data = data[~data['Author'].isin(users["users"])]       
        
        return data

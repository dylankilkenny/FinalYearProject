import pandas as pd
import re
from afinn import Afinn
import numpy as np  
from nltk.corpus import stopwords
import nltk
from collections import Counter


class RedditReducer(object):

    def __init__(self, data, currency_symbols, stopwords):
        self.afinn = Afinn()
        self.stopwords = stopwords
        self.data = self.CleanseData(data)
        self.currency_symbols = currency_symbols


    def CommentsByDay(self):
        cbd = self.data.copy()
        cbd = cbd.groupby(['Date']).size().to_frame('n').reset_index()
        cbd = cbd.to_json(orient='records')
        return cbd
    
    def MostActiveUsers(self):
        mau = self.data.copy()
        mau = mau['Author'].value_counts()
        return mau

    def OverallUserScore(self):
        ous = self.data.copy()
        ous =  ous.groupby('Author')['Score'].sum()
        return ous
    
    def CommentSentiment(self):
        cs = self.data.copy()
        cs['SA'] = np.array([ self.AnalyseSentiment(text) for text in cs['Text'] ])
        self.comment_sentiment = cs
        return cs
    
    def SentimentByDay(self):
        sbd = self.comment_sentiment
        sbd =  sbd.groupby('Date')['SA'].sum()
        return sbd

    def WordCount(self):
        wc = self.data.copy()
        counts = Counter(" ".join(wc['Text']).split()).most_common(300)
        df = pd.DataFrame(counts, columns=['word', 'n'])
        self.word_count = df
        return self.word_count

    def CurrencyMentions(self):
        word_count = self.word_count
        cm = self.currency_symbols.copy()

        cm['Symbol'] = cm.Symbol.str.lower()
        cm['Name'] = cm.Name.str.lower()
        cm["Mentions_Sym"] = 0
        cm["Mentions_Name"] = 0

        for symbol in cm['Symbol']:
            c = word_count.loc[word_count['word'] == symbol, 'count'].values
            if len(c) > 0:
                cm.loc[cm['Symbol'] == symbol, 'Mentions_Sym'] = c[0]

        for symbol in cm['Name']:
            c = word_count.loc[word_count['word'] == symbol, 'count'].values
            if len(c) > 0:
                cm.loc[cm['Name'] == symbol, 'Mentions_Name'] = c[0]
        
        return cm
            
    def CleanText(self, text):
        return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", str(text)).split())
    
    def AnalyseSentiment(self, text):
        analysis = self.CleanText(text)
        return self.afinn.score(analysis)
    
    def CleanseData(self, source):
        #Change date format
        source["Date"] = pd.to_datetime(source["Date"],unit='s')
        #Create df object
        data = {"Author": source["Author"], "Text": source["Body"], "Date": source["Date"], "Score": source["Score"] }
        #Create df
        data = pd.DataFrame(data=data)
        #Convert datetime to date
        data["Date"] = data["Date"].dt.date
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

        return data
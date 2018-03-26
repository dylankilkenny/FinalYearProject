import pandas as pd
import re
from afinn import Afinn
import numpy as np  
from nltk.corpus import stopwords
import nltk


def clean_text(text):
    return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", str(text)).split())

    
def analize_sentiment(text):
    analysis = clean_text(text)
    return afinn.score(analysis)

def load_data():
    return pd.read_csv('../data/comments_btc_2017-01-26_2018-01-26.csv', parse_dates=['Date'])

def cleanse_data(source):
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
    return data

def WordCount(data):
    data['words'] = data.Text.str.strip().str.split('[\W_]+')
    rows = list()
    for row in data[['words']].iterrows():
        r = row[1]
        for word in r.words:
            rows.append((word))
    words = pd.DataFrame(rows, columns=['word'])
    stop = stopwords.words('english')
    words['word'] = words.word.str.lower()
    words['word'] = words['word'].apply(lambda x: ''.join([word for word in x.split() if word not in (stop)]))
    words = words[words.word.str.len() > 0]
    words = words[words.word.str.len() > 1]
    counts = words.word.value_counts().to_frame().reset_index()\
        .rename(columns={'index':'word', 'word':'count'})
    return counts

if __name__ == "__main__":
    afinn = Afinn()

    #Load Dataset
    source = load_data()

    #Clean Dataset
    data = cleanse_data(source)
    print(data)
    print("Dataset Clean!")

    comments_by_day = data.copy()
    comments_by_day = comments_by_day.groupby(['Date']).size()
    print(comments_by_day)
    print("comments_by_day")

    MostActiveUsers = data.copy() 
    MostActiveUsers = MostActiveUsers['Author'].value_counts()
    print(MostActiveUsers)
    print("MostActiveUsers")
    

    user_overall_score = data.copy()
    user_overall_score =  user_overall_score.groupby('Author')['Score'].sum()
    print(user_overall_score)
    print("user_overall_score") 
    

    comment_sentiment = data.copy()
    comment_sentiment['SA'] = np.array([ analize_sentiment(text) for text in comment_sentiment['Text'] ])
    print(comment_sentiment.head(10))
    print("comment_sentiment")
    

    sentiment_by_day = comment_sentiment.copy()
    sentiment_by_day =  sentiment_by_day.groupby('Date')['SA'].sum()
    print(user_overall_score)
    print("user_overall_score")
    

    word_count = data.copy()
    word_count = WordCount(word_count)
    print(word_count)
    print("word_count")
    

    # word_count.to_csv("wordcount.csv", sep=',')
    
    currency_symbols = pd.read_csv('../data/CurrencySymbols.csv')
    currency_symbols['Symbol'] = currency_symbols.Symbol.str.lower()
    currency_symbols['Name'] = currency_symbols.Name.str.lower()
    currency_symbols["Mentions_Sym"] = 0
    currency_symbols["Mentions_Name"] = 0

    for symbol in currency_symbols['Symbol']:
        c = word_count.loc[word_count['word'] == symbol, 'count'].values
        if len(c) > 0:
            currency_symbols.loc[currency_symbols['Symbol'] == symbol, 'Mentions_Sym'] = c[0]

    for symbol in currency_symbols['Name']:
        c = word_count.loc[word_count['word'] == symbol, 'count'].values
        if len(c) > 0:
            currency_symbols.loc[currency_symbols['Name'] == symbol, 'Mentions_Name'] = c[0]
        
    print(currency_symbols)
    print("currency_symbols")
    

            
        
    
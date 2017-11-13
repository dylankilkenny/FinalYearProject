import praw

subreddits = ["CryptoCurrency", "Bitcoin", "btc", "Altcoins", "BitcoinMarkets", "CryptoMarkets"]

# init Reddit instance
reddit = praw.Reddit(client_id='-EoTqkifUHRcgQ', client_secret="S0OuT5JY-NQKu9DBeOtF7gd72To",
                     password='cryptoscraping999', user_agent='USERAGENT',
                     username='CryptoScraper')

for subreddit in subreddits:

    for submission in reddit.subreddit(subreddit).hot(limit=5):
        
        print(submission.title)
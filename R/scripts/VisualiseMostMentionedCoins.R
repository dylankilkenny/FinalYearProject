#Load Dataset
setwd("/Users/dylankilkenny/dev/finalyearproject/data") #<-- change this to where you have downloaded the csv
data <- read.csv("comments.csv")

#Scrub
corpus<-Corpus(VectorSource(data$Body))
corpus<-tm_map(corpus, content_transformer(tolower))
corpus<-tm_map(corpus, removeNumbers)
corpus<-tm_map(corpus, removeWords, stopwords('english'))
corpus<-tm_map(corpus, stemDocument, language = "english") 
corpus<-tm_map(corpus, removePunctuation)


#document term matrix
m <- as.matrix(tdmatrix)
v <- sort(rowSums(m),decreasing=TRUE)
d <- data.frame(word = names(v),freq=v)
head(d, 10)

#Word Cloud
set.seed(1234)
wordcloud(words = d$word, freq = d$freq, min.freq = 1,
          max.words=200, random.order=FALSE, rot.per=0.45, 
          colors=brewer.pal(8, "Dark2"))
#barplot
barplot(d$freq, las = 2, names.arg = d$word,
        col ="lightblue", main ="Most frequent coins",
        ylab = "Coin frequencies")

#document term matrix
tdm<-TermDocumentMatrix(corpus)
tdmatrix<-as.matrix(tdm)

#frequencies
wordfreq<-sort(rowSums(tdmatrix), decreasing = TRUE)
wmat <- as.matrix(wordfreq)
coinlist <- tolower(coinlist)

#most frequent coins
mostpopular <- wmat[coinlist[1:39],1]



#Set Coinlist
coinlist <- c("BTC", "ETH", "BCH", "XRP", "DASH", "LTC", "BTG", 
              "IOTA", "ADA", "XMR", "ETC", "XEM", "NEO", "EOS", 
              "XLM", "BCC", "QTUM", "OMG", "ZEC", "LSK", "USDT", 
              "HSR", "WAVES", "STRAT", "PPT", "ARDR", "NEXT", 
              "MONA", "ARK", "REP", "BTS", "BCN", "DCR", "VTC", 
              "STEEM", "KMD", "PIVX", "SALT", "GNT", "SC", "SNT", 
              "PAY", "POWR", "DOGE", "MAID", "WTC", "BNB", "DGD",
              "VERI", "FCT")


library('tm')
library("wordcloud")

#correlation between words
findAssocs(tdm, terms = "censorship", corlimit = 0.3)

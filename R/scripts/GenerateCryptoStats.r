setwd("/Users/dylankilkenny/documents/college/finalyearproject/r/data")

library(dplyr)
library(tidytext)
library(anytime)
data(stop_words)

#Load data
data <- read.csv("comments_btc_2017-01-26_2018-01-26.csv")

# Standardise Date
data$Date <- as.integer(as.character(data$Date)) %>%
  anytime()%>%
  as.Date()

# Create tidytext df
data <- data_frame(Author = data$Author, Text = data$Body, Date= data$Date, Score =  data$Score )
data$Text <- as.character(data$Text)

# Remove Links
data$Text <- gsub("\\s?(f|ht)(tp)(s?)(://)([^\\.]*)[\\.|/](\\S*)", "", data$Text)

# No Comments per Day
CommentsByDay <- data %>%
  group_by(Date = as.Date(Date)) %>%
  summarise(comments = n()) %>%
  arrange(comments)

# Most active users based on number of comments
MostActiveUsers <- data %>%
  count(Author, sort = TRUE) %>%
  mutate(Author = reorder(Author, n))

# User Overall Score
data$Score <-  as.numeric(data$Score)
data$Author <-  as.factor(data$Author)
UserOveralScore <- data %>%
  group_by(Author)%>%
  summarise(Sum = sum(Score))%>%
  arrange(desc(Sum))

# Seperate into words
data_token <- data %>%
  unnest_tokens(word, Text)

# Remove stop words
data_token <- data_token %>%
  anti_join(stop_words)

# Most Popular Words
MostPopularWords <- data_token %>%
  count(word, sort = TRUE)

# Most Popular Words By author
MostPopularWordsByAuthor <- data_token %>%
  count(word, Author, sort = TRUE)
MostPopularWordsByAuthor <- left_join(MostPopularWordsByAuthor, AuthorNoWords)

# No Words Per Author
AuthorNoWords <- MostPopularWordsByAuthor %>% 
  group_by(Author) %>% 
  summarize(total = sum(n))

#  tf_idf
tf_idf <- MostPopularWordsByAuthor %>%
  bind_tf_idf(word, Author, n)

# N-Grams
n_gram <- data %>%
  unnest_tokens(bigram, Text, token = "ngrams", n = 2)

# N-Grams tf_idf
bigram_tf_idf <- n_gram %>%
  count(Author, bigram) %>%
  bind_tf_idf(bigram, Author, n) %>%
  arrange(desc(tf_idf))

# Sentiment AFINN
afinn <- data_token %>% 
  inner_join(get_sentiments("afinn"))

# Sentiment by Author
AuthorSentimentSum <- afinn %>% 
  group_by(Author) %>% 
  summarize(total = sum(score))

# Sentiment by Date
DateSentimentSum <- afinn %>% 
  group_by(Date) %>% 
  summarize(total = sum(score))

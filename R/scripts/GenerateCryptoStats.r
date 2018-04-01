

library(dplyr)
library(tidytext)
library(anytime)
data(stop_words)

#Load data
setwd("/Users/dylankilkenny/documents/college/finalyearproject/r/data")
data <- read.csv("post_btc_2017-01-26_2018-01-26.csv")
write.csv(file="small_data_post.csv", data[1:2000,])
write.csv(file="stopwords.csv", stop_words[,1])
#coins <- read.csv("coinlist.csv")
#coins$id <- as.character(coins$id)
#coins$Name <- as.character(coins$Name)
#coins$Name <- tolower(coins$Name)
#coins$Symbol <- as.character(coins$Symbol)
#coins <- coins[coins$Rank<250,]

# Standardise Date
data$Date <- as.integer(as.character(data$Date)) %>%
  anytime()%>%
  as.Date()

# Create tidytext df
data <- data_frame(Author = data$Author, Text = data$Body, Date= data$Date, Score =  data$Score )
data$Text <- as.character(data$Text)
data$Author <- as.character(data$Author)
data$Text <- sapply(data$Text, tolower)

# Remove Links
data$Text <- gsub("\\s?(f|ht)(tp)(s?)(://)([^\\.]*)[\\.|/](\\S*)", "", data$Text)

# No Comments per Day
CommentsByDay <- data %>%
  group_by(Date = as.Date(Date)) %>%
  summarise(comments = n()) %>%
  arrange(comments)
write.csv(file="CommentsByDay.csv", CommentsByDay)

# Most active users based on number of comments
MostActiveUsers <- data %>%
  count(Author, sort = TRUE) %>%
  mutate(Author = reorder(Author, n))%>%
  top_n(1000)

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
  count(word, sort = TRUE) %>%
  top_n(1000)

# Most Popular Words By author
MostPopularWordsByAuthor <- data_token %>%
  filter(data_token$Author %in% MostActiveUsers$Author)%>%
  count(word, Author, sort = TRUE) %>%
  group_by(Author)%>%
  top_n(50) 

# No Words Per Author
AuthorNoWords <- MostPopularWordsByAuthor %>% 
  group_by(Author) %>% 
  summarize(total = sum(n))

MostPopularWordsByAuthor <- left_join(MostPopularWordsByAuthor, AuthorNoWords)

#  tf_idf
# tf_idf <- MostPopularWordsByAuthor %>%
#   bind_tf_idf(word, Author, n)

# N-Grams
# n_gram <- data %>%
#   unnest_tokens(bigram, Text, token = "ngrams", n = 3)

# N-Grams tf_idf
# bigram_tf_idf <- n_gram %>%
#   count(Author, bigram) %>%
#   bind_tf_idf(bigram, Author, n) %>%
#   arrange(desc(tf_idf))

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

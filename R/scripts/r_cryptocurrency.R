setwd("/Users/dylankilkenny/documents/college/finalyearproject/r") #<-- change this to where you have downloaded the csv
comments <- read.csv("comments_cryptocurrency_2017-01-26_2018-01-26.csv")
comments <- read.csv("rCryptoComments.csv")
save(comments, file="r_cryptocurrency.RData")
load("r_cryptocurrency.RData")
library(dplyr)
library(tidytext)
library(anytime)
data(stop_words)

colnames(comments) <- c("ID", "PostID", "Body", "Date", "Score", "Author")
comments <- comments[-3,]
sapply(comments,function(x) sum(is.na(x)))
any(is.na(test))
comments$Date <- as.integer(as.character(comments$Date)) %>%
  anytime()%>%
  as.Date()
save(comments, file="r_cryptocurrency.RData")
load("r_cryptocurrency.RData")

comments_tokens <- data.frame()



text_df %>%
  filter(Author == "senzheng") %>%
  count(word, sort = TRUE) 

comments %>%
  filter(Author == "DestroyerOfShitcoins") %>%
  count(Author, sort = TRUE) 

####################
#Most popular words#
####################

library(ggplot2)

text_df <- data_frame(Author = comments$Author, text = comments$Body)
text_df$text <- as.character(text_df$text)

# Seperate into words
text_df <- text_df %>%
  unnest_tokens(word, text)

# Remove stop words
text_df <- text_df %>%
  anti_join(stop_words)

text_df %>%
  count(word, sort = TRUE) %>%
  filter(n > 600) %>%
  top_n(20) %>%
  mutate(word = reorder(word, n)) %>%
  ggplot(aes(word, n)) +
  geom_col() +
  xlab(NULL) +
  coord_flip()

write.csv(file="tokenisedCryptocurrencyComments.csv", text_df)

########################################################
#Comments on r/cryptocurrency over the course of 1 year#
########################################################
commentsByDay <- data_frame(ID = comments$ID, Date = comments$Date)
commentsByDay$ID <- as.character(commentsByDay$ID)

commentsByDay %>%
  group_by(Date = as.Date(Date)) %>%
  summarise(comments = n()) %>%
  arrange(comments) %>%
  ggplot(aes(Date, comments)) +
  geom_col() +
  scale_x_date(date_breaks = "2 month", date_labels =  "%b %Y")

commentsByDay %>%
  count(Date, sort = TRUE) %>%
  filter(n > 600) %>%
  top_n(20)

###########
#Top Users#
###########
comments %>%
  count(Author, sort = TRUE) %>%
  filter(n > 600) %>%
  top_n(20) %>%
  mutate(Author = reorder(Author, n)) %>%
  ggplot(aes(Author, n)) +
  geom_col()+
  coord_flip()

#############
# Sentiment #
#############

sent_df <- data_frame(Author = comments$Author, text = comments$Body, Date = comments$Date)
sent_df$text <- as.character(sent_df$text)

# Remove links
sent_df$text <- gsub("\\s?(f|ht)(tp)(s?)(://)([^\\.]*)[\\.|/](\\S*)", "", sent_df$text)

# Seperate into words
sent_df <- sent_df %>%
  unnest_tokens(word, text)

# Remove stop words
sent_df <- sent_df %>%
  anti_join(stop_words)

library(tidyr)

#Sentiment over 1 year
sent_df_total <- sent_df %>%
  inner_join(get_sentiments("bing")) %>%
  count(Author, index = Date, sentiment) %>%
  spread(sentiment, n, fill = 0) %>%
  mutate(sentiment = positive - negative)

ggplot(sent_df_total, aes(index, sentiment)) +
  geom_col(show.legend = FALSE)

# Top users overall positive sentiment
sent_df_total %>%
  arrange(sentiment)%>%
  top_n(20)%>%
  ggplot(aes(Author, sentiment)) +
  geom_col() +
  coord_flip()

# User sentiment
sent_df_total <- sent_df %>%
  inner_join(get_sentiments("bing")) %>%
  count(Author, sentiment) %>%
  spread(sentiment, n, fill = 0) %>%
  mutate(sentiment = positive - negative)

sent_df_total %>%
  filter(Author == "thepaip")

#### compare words to cryptonames ####
coinlist <- read.csv("coinlist.csv")
coinlist$id <- as.character(coinlist$id)%>%
  tolower()
coinlist$Name <- as.character(coinlist$Name)%>%
  tolower()
coinlist$Symbol <- as.character(coinlist$Symbol)%>%
  tolower()

coin_top_200 <- coinlist %>%
  filter(Rank <= 200)

text_df <- text_df %>%
  count(word, sort = TRUE)

text_df %>%
  filter(word %in% coin_top_200$id)

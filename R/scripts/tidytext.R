setwd("/Users/dylankilkenny/documents/college/finalyearproject/r") #<-- change this to where you have downloaded the csv
comments <- read.csv("comments_cryptocurrency_2017-01-26_2018-01-26.csv")
text_df <- read.csv("tokenisedCryptocurrencyComments.csv")

install.packages("tidytext")
install.packages("anytime")

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
write.csv(file="rCryptoComments.csv", text_df)

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

#############################
#####  R/Bitcoin ##########
r_bitcoin_1 <- read.csv("comments_bitcoin_2017-01-26_2017-05-22.csv")
r_bitcoin_2 <- read.csv("comments_bitcoin_2017-01-26_2018-01-26.csv")

bitcoin <- rbind(r_bitcoin_1, r_bitcoin_2)

bitcoin$Date <- as.integer(as.character(bitcoin$Date)) %>%
  anytime()%>%
  as.Date()

write.csv(file="r_bitcoin.csv", bitcoin)

bitcoin_df <- data_frame(Author = bitcoin$Author, text = bitcoin$Body, date= bitcoin$Date)
bitcoin_df$text <- as.character(bitcoin_df$text)
bitcoin_df$text <- gsub("\\s?(f|ht)(tp)(s?)(://)([^\\.]*)[\\.|/](\\S*)", "", bitcoin_df$text)


# Seperate into tokens
bitcoin_df <- bitcoin_df %>%
  unnest_tokens(word, text)

# Remove stop words
bitcoin_df <- bitcoin_df %>%
  anti_join(stop_words)

#count
bitcoin_wordcount <- bitcoin_df %>%
  count(word, sort = TRUE)

#Most popular words plot
bitcoin_df %>%
  count(word, sort = TRUE) %>%
  filter(n > 600) %>%
  top_n(20) %>%
  mutate(word = reorder(word, n)) %>%
  ggplot(aes(word, n)) +
  geom_col() +
  xlab(NULL) +
  coord_flip()

#comments over course of 1 year
bitcoin_df %>%
  group_by(Date = as.Date(date)) %>%
  summarise(comments = n()) %>%
  arrange(comments) %>%
  ggplot(aes(Date, comments)) +
  geom_line() +
  scale_x_date(date_breaks = "1 month", date_labels =  "%b %Y")

bitcoin_df %>%
  count(date, sort = TRUE) %>%
  filter(n > 600) %>%
  top_n(20)


#Historical Price Data
price_data <- read.csv("HistoricalCoinData.csv")
price_data$Market.Cap <- as.numeric(price_data$Market.Cap)
price_data$Date <- as.character(price_data$Date)
price_data$Date <- price_data$Date%>%
  anytime()%>%
  as.Date()

#Fix NA
any(is.na(price_data))
price_data <- price_data[!is.na(price_data$Date),]
price_data$Coin <- as.character(price_data$Coin)

#Get bitcoin prices
bitcoin_prices <- price_data %>%
  filter(Coin == "bitcoin")

#change datatype
bitcoin_prices$Close <- as.numeric(as.character(bitcoin_prices$Close))

#Bitcoin price
ggplot(bitcoin_price_comment, aes(Date, Close)) +
  geom_line() +
  scale_x_date(date_breaks = "1 month", date_labels =  "%b %Y")

#price vs post replies
bitcoin_price_comment <- bitcoin_df %>%
  count(date, sort = TRUE)

colnames(bitcoin_price_comment)[1] <- "Date"

bitcoin_price_comment <- inner_join(bitcoin_price_comment, bitcoin_prices)

cor(bitcoin_price_comment$n, bitcoin_price_comment$Close)

ggplot(bitcoin_price_comment, aes(x = Date)) +
  geom_line(aes(y = n/10, colour = "Comment Count (n/10)")) +
  geom_line(aes(y = Close, colour = "Bitcoin price")) + 
  scale_colour_manual(values = c("black", "orange"))+
  scale_y_continuous(sec.axis = sec_axis(~. *10, name = "Bitcoin price"))+
  labs(y = "Comment Count on r/bitcoin",
       x = "Date",
       colour = "Legend")+
  scale_x_date(date_breaks = "1 month", date_labels =  "%b %Y")+ 
  theme(legend.position = c(0.2, 0.8))


require(scales)
#Most popular users
bitcoin %>%
  count(Author, sort = TRUE) %>%
  filter(n > 600 & n < 20000) %>%
  top_n(20) %>%
  mutate(Author = reorder(Author, n)) %>%
  ggplot(aes(Author, n)) +
  scale_y_continuous(labels = comma)+
  geom_col()+
  coord_flip()

top_10_bitcoin <- bitcoin %>%
  count(Author, sort = TRUE) %>%
  filter(n > 600 & n < 20000) %>%
  top_n(10) 

####################
###### r/btc #######

r_btc <- read.csv("comments_btc_2017-01-26_2018-01-26.csv")
summary(r_btc)

r_btc$Date <- as.integer(as.character(r_btc$Date)) %>%
  anytime()%>%
  as.Date()

write.csv(file="r_btc.csv", r_btc)

btc_df <- data_frame(Author = r_btc$Author, Text = r_btc$Body, Date= r_btc$Date, Score =  r_btc$Score )
btc_df$Text <- as.character(btc_df$Text)
btc_df$Text <- gsub("\\s?(f|ht)(tp)(s?)(://)([^\\.]*)[\\.|/](\\S*)", "", btc_df$Text)

library(ggplot2)
#Comments over 1 year
btc_df %>%
  group_by(Date = as.Date(Date)) %>%
  summarise(comments = n()) %>%
  arrange(comments) %>%
  ggplot(aes(Date, comments)) +
  geom_line() +
  scale_x_date(date_breaks = "1 month", date_labels =  "%b %Y")

btc_df %>%
  count(Date, sort = TRUE) %>%
  filter(n > 600) %>%
  top_n(20)
require(scales)

#Most popular users
btc_df %>%
  count(Author, sort = TRUE) %>%
  filter(n > 600 & n < 20000) %>%
  top_n(20) %>%
  mutate(Author = reorder(Author, n)) %>%
  ggplot(aes(Author, n)) +
  scale_y_continuous(labels = comma)+
  geom_col()+
  coord_flip()

top_10_bitcoin <- btc_df %>%
  count(Author, sort = TRUE) %>%
  filter(n > 600 & n < 20000) %>%
  top_n(10) 

btc_df$Score <-  as.numeric(btc_df$Score)
btc_df$Author <-  as.factor(btc_df$Author)
btc_df %>%
  group_by(Author)%>%
  summarise(Sum = sum(Score))%>%
  arrange(desc(Sum))%>%
  top_n(10)

btc_df %>%
  filter(Author == "JTW24")%>%
  str()

# Seperate into words
btc_token <- btc_df %>%
  unnest_tokens(word, Text)

# Remove stop words
btc_token <- btc_token %>%
  anti_join(stop_words)

?sort

sum(1, -2)

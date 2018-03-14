setwd("/Users/dylankilkenny/documents/college/finalyearproject/r") #<-- change this to where you have downloaded the csv

library(dplyr)
library(tidytext)
library(anytime)
require(scales)
library(tidyr)
library(topicmodels)
library(ggplot2)
data(stop_words)

#############################
#####  R/Bitcoin ##########
r_bitcoin_1 <- read.csv("comments_bitcoin_2017-01-26_2017-05-22.csv")
r_bitcoin_2 <- read.csv("comments_bitcoin_2017-01-26_2018-01-26.csv")

bitcoin <- rbind(r_bitcoin_1, r_bitcoin_2)

bitcoin$Date <- as.integer(as.character(bitcoin$Date)) %>%
  anytime()%>%
  as.Date()

write.csv(file="r_bitcoin.csv", bitcoin)
r_bitcoin <- read.csv("r_bitcoin.csv")
save(r_bitcoin, file="r_bitcoin.RData")
load("r_bitcoin.RData")

bitcoin_df <- data_frame(Author = r_bitcoin$Author, text = r_bitcoin$Body, date= r_bitcoin$Date)
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

bitcoin_price_comment$Date <- bitcoin_price_comment$Date%>%
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

#### tf-idf ####
# Seperate into tokens
bitcoin_df <- bitcoin_df %>%
  unnest_tokens(word, text)

# Remove stop words
bitcoin_df <- bitcoin_df %>%
  anti_join(stop_words)

#count
bitcoin_wordcount <- bitcoin_df %>%
  count(Author,word, sort = TRUE)

total_words <- bitcoin_wordcount %>% 
  group_by(Author) %>% 
  summarize(total = sum(n))

bitcoin_wordcount <- left_join(bitcoin_wordcount, total_words)

#Remove deleted comments
bitcoin_wordcount <- bitcoin_wordcount %>%
  filter(Author != "")



total_words %>%
  top_n(10)%>%
  arrange(desc(total))%>%
  right_join(bitcoin_wordcount)%>%
  ggplot(aes(n/total, fill = Author)) +
  geom_histogram(show.legend = FALSE,bins=10) +
  xlim(NA, 0.0009) +
  facet_wrap(~Author, ncol = 2, scales = "free_y")

bitcoin_wordcount <- bitcoin_wordcount %>%
  bind_tf_idf(word, Author, n)
bitcoin_wordcount

bitcoin_wordcount %>%
  select(-total) %>%
  arrange(desc(tf_idf))
?rev

total_words %>%
  top_n(6)%>%
  arrange(desc(total))%>%
  inner_join(bitcoin_wordcount)%>%
  mutate(word = factor(word, levels = rev(unique(word)))) %>% 
  group_by(Author) %>% 
  top_n(15) %>% 
  ungroup %>%
  ggplot(aes(word, tf_idf, fill = Author)) +
  geom_col(show.legend = FALSE) +
  labs(x = NULL, y = "tf-idf") +
  facet_wrap(~Author, ncol = 2, scales = "free") +
  coord_flip()

#### n-grams ####

bitcoin_df <- data_frame(Author = r_bitcoin$Author, text = r_bitcoin$Body, date= r_bitcoin$Date)
bitcoin_df$text <- as.character(bitcoin_df$text)
bitcoin_df$text <- gsub("\\s?(f|ht)(tp)(s?)(://)([^\\.]*)[\\.|/](\\S*)", "", bitcoin_df$text)

bitcoin_bigrams <- bitcoin_df %>%
  unnest_tokens(bigram, text, token = "ngrams", n = 2)
bitcoin_bigrams



bitcoin_bigrams <- bitcoin_bigrams %>%
  separate(bigram, c("word1", "word2"), sep = " ") %>%
  filter(!word1 %in% stop_words$word) %>%
  filter(!word2 %in% stop_words$word) %>% 
  count(word1, word2, sort = TRUE)

bitcoin_bigrams

bigrams_united <- bitcoin_bigrams %>%
  unite(bigram, word1, word2, sep = " ")


bigram_tf_idf <- bigrams_united %>%
  count(book, bigram) %>%
  bind_tf_idf(bigram, book, n) %>%
  arrange(desc(tf_idf))

bigram_tf_idf

#### topic modeling ####
b <- data_frame(Document = "bitcoin", Text = r_bitcoin$Body)

b$Text <- as.character(b$Text)

b <- b %>%
  unnest_tokens(word, Text)

b <- b %>%
  count(Document, word, sort = TRUE)

b <- b %>%
  anti_join(stop_words)

c <- data_frame(Document = "btc", Text = c$Body)
c$Text <- as.character(c$Text)

c <- c %>%
  unnest_tokens(word, Text)%>%
  count(Document, word, sort = TRUE)

c <- c %>%
  anti_join(stop_words)

combined <- rbind(b, c)

rm(b)
rm(c)
rm(r_bitcoin)
rm(r_btc)

summary(combined)

combined <- combined%>%
  cast_dtm(Document, word, n)

combined_lda <- LDA(combined, k = 2, control = list(seed = 1234))
combined_topics <- tidy(combined_lda, matrix = "beta")
combined_topics

combined_top_terms <- combined_topics %>%
  group_by(topic) %>%
  top_n(10, beta) %>%
  ungroup() %>%
  arrange(topic, -beta)

combined_top_terms %>%
  mutate(term = reorder(term, beta)) %>%
  ggplot(aes(term, beta, fill = factor(topic))) +
  geom_col(show.legend = FALSE) +
  facet_wrap(~ topic, scales = "free") +
  coord_flip()

beta_spread <- combined_topics %>%
  mutate(topic = paste0("topic", topic)) %>%
  spread(topic, beta) %>%
  filter(topic1 > .001 | topic2 > .001) %>%
  mutate(log_ratio = log2(topic2 / topic1))

beta_spread %>%
  arrange(desc(log_ratio))%>%
  ggplot(aes(term, log_ratio)) +
  geom_col(show.legend = FALSE) +
  coord_flip()


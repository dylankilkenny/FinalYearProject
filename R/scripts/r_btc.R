setwd("/Users/dylankilkenny/documents/college/finalyearproject/r") #<-- change this to where you have downloaded the csv

library(dplyr)
library(tidytext)
library(anytime)
data(stop_words)

####################
###### r/btc #######

#r_btc <- read.csv("comments_btc_2017-01-26_2018-01-26.csv")
r_btc <- read.csv("r_btc.csv")
summary(r_btc)
save(r_btc, file="r_btc.RData")
load("r_btc.RData")
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

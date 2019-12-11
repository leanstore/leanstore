library(ggplot2)
library(sqldf)

d=read.csv('50_rounds.txt')

sqldf("select * from d limit 10")

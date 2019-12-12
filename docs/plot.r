library(ggplot2)
library(sqldf)

d=read.csv('50_rounds.txt')

sqldf("select * from d limit 10")

plot(d$txs)

plot(d$rio)

plot(d$wmibs)

plot(d$uns)

plot(d$poll)

plot(d$swi)

plot(d$as)



plot(d$wmibs/d$txs)

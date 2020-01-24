library(ggplot2)
library(sqldf)

dev.set(2)

d=read.csv('/home/adnan/rome/dev/leanstore/release/frontend/zipf.csv')
c=sqldf("select k, count(*) cnt from d group by k")
head(c)

sqldf("select * from c limit 100")

old=read.csv('/home/adnan/rome/dev/leanstore/release/frontend/zipf2.csv')
sqldf("select k, count(*) from old group by k limit 10")

head(d)
sqldf("select k, count(*) from d group by k limit 10")


ggplot(d, aes(x=k)) +
  geom_histogram(color="black", fill="white", bins = 100)

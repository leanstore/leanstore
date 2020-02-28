library(ggplot2)
library(sqldf)
library(Cairo)
library(stringr)
library(scales)
# TPC-C A: 100 warehouse, in-memory, variable threads, with/-out split&merge

dev.set(0)
df=read.csv('./test.csv')

d=sqldf("select * from df")
tx <- ggplot(d, aes(t, tx, color=factor(c_worker_threads), group=interaction(c_worker_threads, c_cm_split))) + geom_line() + geom_point(aes(shape=factor(c_cm_split)))
tx <- tx
print(tx)


d=sqldf("select c_worker_threads,c_cm_split,max(tx) tx from df group by c_worker_threads, c_cm_split")
tx <- ggplot(d, aes(x=c_worker_threads, y=tx, fill=factor(c_cm_split))) + geom_bar(stat="identity", position=position_dodge()) + scale_x_discrete(limits(30,60,90,100,120))
print(tx)

CairoPDF("./A.pdf", bg="transparent")
dev.off()

library(ggplot2)
library(sqldf)
library(Cairo)
library(stringr)
library(scales)
# TPC-C A: 100 warehouse, in-memory, variable threads, with/-out split&merge

dev.set(0)
df=read.csv('./test.csv')

d=sqldf("select c_worker_threads,c_cm_split,max(tx) tx from df group by c_worker_threads, c_cm_split")
tx <- ggplot(d, aes(x=factor(c_worker_threads), y=tx, fill=factor(c_cm_split))) + geom_bar(stat="identity", position=position_dodge()) + scale_x_discrete(name="worker threads") + scale_y_continuous(name="TPC-C throughput [txn/s]") + scale_fill_discrete(name=NULL, labels=c("baseline","+split +merge"))
print(tx)

CairoPDF("./tpcc_A.pdf", bg="transparent")
print(tx)
dev.off()

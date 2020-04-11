library(ggplot2)
library(sqldf)
library(Cairo)
library(stringr)
library(scales)
# TPC-C A: 100 warehouse, in-memory, variable threads, with/-out split&merge

dev.set(0)
#df=read.csv('./A_rome_pin.csv')
df=read.csv('./A_mutex.csv')
sqldf("select distinct c_worker_threads from df")
df$c_mutex <- ordered(df$c_mutex,levels=c(0,1), labels=c("Spin", "Mutex"))
d=sqldf("select c_pin_threads,c_worker_threads,c_cm_split,max(tx) tx from df group by c_worker_threads, c_cm_split,c_pin_threads")

tx <- ggplot(d, aes(x=factor(c_worker_threads), y =tx, color=factor(c_cm_split), group=factor(c_cm_split))) +
    geom_point() +
    scale_x_discrete(name="worker threads") +
    scale_y_continuous(name="TPC-C throughput [txn/s]") +
    scale_color_discrete(name=NULL, labels=c("baseline","+CS +EM")) +
    geom_line() +
    expand_limits(y=0) +
    theme_bw() +
    facet_grid(row=vars(),col=vars())
print(tx)

CairoPDF("./tpcc_A_intel.pdf", bg="transparent", height=3)
print(tx)
dev.off()

speedup = sqldf("select s.c_worker_threads,s.tx * 1.0 /b.tx from d s, d b where s.c_cm_split= 1 and b.c_cm_split=0 and b.c_worker_threads=s.c_worker_threads group by s.c_worker_threads")
speedup

df=read.csv('./A_skx.csv')
d=sqldf("select c_worker_threads,c_cm_split,max(tx) tx from df group by c_worker_threads, c_cm_split")
tx <- ggplot(d, aes(x=factor(c_worker_threads), y =tx, color=factor(c_cm_split), group=factor(c_cm_split))) + geom_point() + scale_x_discrete(name="worker threads") + scale_y_continuous(name="TPC-C throughput [txn/s]") + scale_color_discrete(name=NULL, labels=c("baseline","+CS +EM")) + geom_line() + expand_limits(y=0) + theme_bw()
print(tx)

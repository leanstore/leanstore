library(ggplot2)
library(sqldf)
library(Cairo)
library(stringr)
library(scales)
# TPC-C A: 100 warehouse, in-memory, variable threads, with/-out split&merge

dev.set(0)
df=read.csv('./A_mutex.csv')
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










arm=read.csv('./Arm_stats.csv')
arm=sqldf("select c_pin_threads,c_worker_threads, c_cm_split,max(tx) tx from arm group by c_worker_threads, c_cm_split,c_pin_threads")
aws=read.csv('./aws/tpcc_a_stats.csv')
aws=sqldf("select c_pin_threads,c_worker_threads,c_cm_split,max(tx) tx from aws group by c_worker_threads, c_cm_split,c_pin_threads")
d=sqldf("select *,'m6g.16xlarge' as tag from arm union select *,'c5.18xlarge' as tag from aws")

tx <- ggplot(d, aes(x=factor(c_worker_threads), y =tx, color=factor(c_cm_split), group=factor(c_cm_split))) +
    geom_point() +
    scale_x_discrete(name="worker threads") +
    scale_y_continuous(name="TPC-C throughput 100 warehouses [txn/s]") +
    scale_color_discrete(name=NULL, labels=c("baseline","+Contention Split")) +
    geom_line() +
    expand_limits(y=0) +
    theme_bw() +
    facet_grid(row=vars(tag),col=vars())
print(tx)

CairoPDF("./arm_vs_x64.pdf", bg="transparent")
print(tx)
dev.off()

# arm 5,54009  rome 7,64342 (1 warehouse)

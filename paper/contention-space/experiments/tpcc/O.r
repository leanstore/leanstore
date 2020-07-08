library(ggplot2)
library(sqldf)
library(Cairo)
library(stringr)
library(scales)
# TPC-C O: 2000 warehouses

dev.set(0)

df=read.csv('./O_stats.csv')
df=sqldf("select * from df where t >0")
d= sqldf("
select *, 1 as symbol from df where c_su_merge=0 and c_cm_split=0
UNION select *, 2 as symbol from df where c_su_merge=0 and c_cm_split=1
UNION select *, 3 as symbol from df where c_su_merge=1 and c_cm_split=0
UNION select *, 4 as symbol from df where c_su_merge=1 and c_cm_split=1
")

g <- ggplot(d, aes(t, tx, color=factor(symbol), group=factor(symbol))) +
    geom_point(aes(shape=factor(symbol)), size=0.5, alpha=0.5) +
    scale_size_identity(name=NULL) +
    scale_shape_discrete(name=NULL, breaks=c(1,2,3,4), labels=c("base", "+Contention Split","+XMerge","+Contention Split +XMerge")) +
    scale_color_discrete(name =NULL, labels=c("base", "+Contention Split","+XMerge","+Contention Split +XMerge"), breaks=c(1,2,3,4)) +
    labs(x='Processed M Transactions [txn]', y = 'TPC-C throughput [txns/sec]') +
    geom_smooth(method ="auto", size=0.5, se=FALSE) +
    theme_bw() +
    theme(legend.position = 'top') +
    expand_limits(y=0) +
    facet_grid(row=vars(c_worker_threads), scales="free")
print(g)

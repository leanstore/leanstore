source("../common.r", local = TRUE)
setwd("../tpcc")
# TPC-C Out of memory (O)

dev.set(0)
df=read.csv('./O_long_stats.csv')
df=sqldf("select * from df where t >0")
d= sqldf("
select *, 1 as symbol from df where c_su_merge=0 and c_cm_split=0
UNION select *, 2 as symbol from df where c_su_merge=0 and c_cm_split=1
UNION select *, 3 as symbol from df where c_su_merge=1 and c_cm_split=0
UNION select *, 4 as symbol from df where c_su_merge=1 and c_cm_split=1
")

dev.new()
g <- ggplot(d, aes(t, tx, color=factor(symbol), group=factor(symbol))) +
    geom_point(aes(shape=factor(symbol)), size=0.5, alpha=0.5) +
    scale_size_identity(name=NULL) +
    scale_shape_discrete(name=NULL, breaks=c(1,2,3,4), labels=c("base", "+Contention Split","+XMerge","+Contention Split +XMerge")) +
    scale_color_discrete(name =NULL, labels=c("base", "+Contention Split","+XMerge","+Contention Split +XMerge"), breaks=c(1,2,3,4)) +
    labs(x='Time [sec]', y = 'TPC-C throughput [txns/sec]') +
    geom_smooth(method ="auto", size=0.5, se=FALSE) +
    theme_bw() +
    theme(legend.position = 'top') +
    expand_limits(y=0) +
    facet_grid(row=vars(tpcc_warehouse_count), scales="free", labeller = label_both)
print(g)


ggplot(d, aes (t, (w_mib)/tx, color=factor(symbol))) + geom_smooth() + expand_limits(y=0) + facet_grid(rows=vars(tpcc_warehouse_count))
dev.new()
ggplot(d, aes (t, (r_mib)/tx, color=factor(symbol))) + geom_smooth() + expand_limits(y=0) + facet_grid(rows=vars(tpcc_warehouse_count))

stats=read.csv('./C_stats.csv')
dts=read.csv('./C_dts.csv')

dts=sqldf("select * from stats where tpcc_warehouse_count=10000 and c_su_merge=false")

combined=sqldf("select d.*, s.tpcc_warehouse_count, s.c_su_merge from dts d, stats s where s.c_hash = d.c_hash")

sqldf("select dt_name, c_hash, max(dt_misses_counter) from merge group by c_hash, dt_name")

ggplot(dts, aes(t, dt_misses_counter)) + geom_line() + facet_grid (row=vars(dt_name), col=vars()) + scale_y_log10()

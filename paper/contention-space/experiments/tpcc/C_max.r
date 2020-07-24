source("../common.r", local = TRUE)
setwd("../tpcc")
# TPC-C C: 100 warehouses, 120 threads
# 4 combinations of enabled/disabled space/contention management
                                        # C_short_threads.csv C_new.csv have the latest data

dev.set(0)

df=read.csv('./C_max_stats.csv')
df=sqldf("select * from df where t >0 and tpcc_warehouse_count>2000")
d= sqldf("
select *, 1 as variant from df where c_su_merge=0 and c_cm_split=0
UNION select *, 2 as variant from df where c_su_merge=0 and c_cm_split=1
UNION select *, 3 as variant from df where c_su_merge=1 and c_cm_split=0
UNION select *, 4 as variant from df where c_su_merge=1 and c_cm_split=1
")
max=sqldf("select tpcc_warehouse_count, c_su_merge, variant, max(tx) tx, min(space_usage_gib) from d group by tpcc_warehouse_count, c_su_merge, variant")
max

tx <- ggplot(d, aes(x=t,y= tx, color=factor(variant), group=factor(variant))) +
    geom_point(aes(shape=factor(variant)), size=0.5, alpha=0.5) +
    geom_line()+
    scale_size_identity(name=NULL) +
    scale_shape_discrete(name=NULL, labels=labelByVariant, breaks=breakByVariant) +
    scale_color_manual(name =NULL, labels=labelByVariant, values=colorByVariant, breaks=breakByVariant) +
    labs(x='TPC-C Warehouses', y = 'TPC-C Max Throughput [txns/sec]') +
    theme_bw() +
    theme(legend.position = 'top') +
    expand_limits(y=0, x=0) +
   facet_grid(col=vars(), row=vars(tpcc_warehouse_count), scales="free", labeller = label_both)
print(tx)


tx <- ggplot(max, aes(x=tpcc_warehouse_count,y= tx, color=factor(variant), group=factor(variant))) +
    geom_point(aes(shape=factor(variant)), size=0.5, alpha=0.5) +
    geom_line()+
    scale_size_identity(name=NULL) +
    scale_shape_discrete(name=NULL, labels=labelByVariant, breaks=breakByVariant) +
    scale_color_manual(name =NULL, labels=labelByVariant, values=colorByVariant, breaks=breakByVariant) +
    labs(x='TPC-C Warehouses', y = 'TPC-C Max Throughput [txns/sec]') +
    theme_bw() +
    theme(legend.position = 'top') +
    expand_limits(y=0, x=0) +
   facet_grid(col=vars(), row=vars(), scales="free", labeller = label_both)
print(tx)

speedup=sqldf("select m1.tpcc_warehouse_count, m2.tx * 1.0/m1.tx s from max m1,max m2 where m1.tpcc_warehouse_count=m2.tpcc_warehouse_count and m1.c_su_merge=false and m2.c_su_merge=true")
speedup
ggplot(speedup,aes(x=tpcc_warehouse_count, y=s)) + geom_point() + geom_line()

tx <- ggplot(max, aes(x=tpcc_warehouse_count,y= tx, color=factor(variant), group=factor(variant))) +
    geom_point(aes(shape=factor(variant)), size=0.5, alpha=0.5) +
    geom_line()+
    scale_size_identity(name=NULL) +
    scale_shape_discrete(name=NULL, labels=labelByVariant, breaks=breakByVariant) +
    scale_color_manual(name =NULL, labels=labelByVariant, values=colorByVariant, breaks=breakByVariant) +
    labs(x='TPC-C Warehouses', y = 'TPC-C Max Throughput [txns/sec]') +
    theme_bw() +
    theme(legend.position = 'top') +
    expand_limits(y=0, x=0) +
   facet_grid(col=vars(), row=vars(), scales="free", labeller = label_both)
print(tx)

ggplot(d, aes(x=t, y=(r_mib/tx)))+ geom_point() + geom_line() + facet_grid( ~ c_su_merge)

ggplot(d, aes(x=t, y=(w_mib/tx)))+ geom_point() + geom_line() + facet_grid( ~ c_su_merge)


stats=d
dts=read.csv('./C_adhoc_dts.csv')

merged=sqldf("select dts.*, stats.c_cm_split from dts,stats where stats.c_hash = dts.c_hash and stats.t=dts.t")
ggplot(merged, aes(t, cm_split_succ_counter)) + facet_grid(col=vars(c_cm_split), row=vars(dt_name)) + geom_line()




ggplot(dts[dts$dt_name=='order_wdc', ], aes(t, dt_misses_counter)) + facet_grid(col=vars(c_hash), row=vars(dt_name)) + geom_line()

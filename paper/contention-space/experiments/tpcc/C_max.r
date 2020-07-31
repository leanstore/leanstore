source("../common.r", local = TRUE)
setwd("../tpcc")
# TPC-C C: 100 warehouses, 120 threads
# 4 combinations of enabled/disabled space/contention management
                                        # C_short_threads.csv C_new.csv have the latest data

dev.set(0)

df=read.csv('./C_all_stats.csv')
df=sqldf("select * from df where t > 0 and tpcc_warehouse_count in (10,30,60,100,150,200,300,400,500,1000,2500,5000,7500,10000)")
d= sqldf("
select *, 1 as variant from df where c_su_merge=0 and c_cm_split=0
UNION select *, 2 as variant from df where c_su_merge=0 and c_cm_split=1
UNION select *, 3 as variant from df where c_su_merge=1 and c_cm_split=0
UNION select *, 4 as variant from df where c_su_merge=1 and c_cm_split=1
")

max=sqldf("select tpcc_warehouse_count, variant, median(r_mib/tx)*1024 r, median(w_mib/tx)*1024 w, median((w_mib+r_mib)/tx)*1024 io, max(tx) tx, sum(tx) total_tx, min(space_usage_gib) min_gib, max(space_usage_gib) max_gib from d group by tpcc_warehouse_count, variant")
max


besttime=sqldf("select d1.tpcc_warehouse_count, d1.variant, d1.t, d1.space_usage_gib after_gib, d1.tx from d d1 where not exists (select * from d d2 where d2.tpcc_warehouse_count=d1.tpcc_warehouse_count and d2.variant =d1.variant and d2.tx > d1.tx) group by d1. variant, d1.tpcc_warehouse_count, d1.space_usage_gib ")
t

kib_tx=sqldf("select tpcc_warehouse_count, variant, 1024*1024*(max_gib-min_gib)/total_tx kib from max where min_gib > 240 group by variant, tpcc_warehouse_count order by tpcc_warehouse_count asc")
sqldf("select variant, avg(kib) kib from kib_tx group by variant")

sqldf("select d1.tpcc_warehouse_count, d1.variant, d1.t, d1.tx from d d1 where not exists (select * from d d2 where d2.tpcc_warehouse_count=d1.tpcc_warehouse_count and d2.variant =d1.variant and d2.tx > d1.tx) group by d1. variant, d1.tpcc_warehouse_count ")

ggplot(kib_tx, aes(x=tpcc_warehouse_count, y=kib, color=factor(variant))) + geom_point() + geom_line() + scale_color_manual(name =NULL, labels=labelByVariant, values=colorByVariant, breaks=breakByVariant) + scale_x_log10()

tx <- ggplot(max, aes(x=tpcc_warehouse_count,y= tx/1e6, color=factor(variant), group=factor(variant))) +
    geom_point(aes(shape=factor(variant)), size=4/.pt, alpha=1.0) +
    geom_line(size=1.75/.pt) +
    scale_shape_manual(name=NULL, values=shapeByVariant, labels=labelByVariant, breaks=breakByVariant) +
    scale_color_manual(name =NULL, labels=labelByVariant, values=colorByVariant, breaks=breakByVariant) +
    labs(x='TPC-C Warehouses', y = 'TPC-C Throughput [M txns/sec]') +
    theme_acm +
    geom_jitter()+
    scale_x_log10()+
    theme(legend.position = 'top', legend.margin=margin(c(-5,0,-7,0)), axis.title.x = element_blank(), axis.text.x=element_blank()) +
    expand_limits(y=0, x=0) +
    theme(axis.title.y =element_text(size=7)) +
    guides(shape = guide_legend(ncol=2,nrow = 2, byrow=TRUE))
tx
io <- ggplot(max, aes(x=tpcc_warehouse_count,y= io, color=factor(variant), group=factor(variant))) +
    geom_point(aes(shape=factor(variant)), size=4/.pt, alpha=1.0) +
    geom_line(size=1.75/.pt) +
    scale_shape_manual(name=NULL, labels=labelByVariant, values=shapeByVariant, breaks=breakByVariant, guide =FALSE) +
    scale_color_manual(name =NULL, labels=labelByVariant, values=colorByVariant, breaks=breakByVariant, guide =FALSE) +
    labs(x='TPC-C Warehouses', y = 'IO (Reads + Writes) [KiB/tx]') +
    theme_acm +
    theme(axis.title.y =element_text(size=7)) +
    geom_jitter()+
    scale_x_log10()+
    expand_limits(y=0, x=0)
io
g2 <- ggplotGrob(tx)
g3 <- ggplotGrob(io)
g <- rbind(g2, g3,  size = "first")
g$widths <- unit.pmax(g2$widths, g3$widths)
grid.newpage()
grid.draw(g)
ggsave('../../tex/figures/tpcc_C.pdf', plot =g, width=lineWidthInInches , height = 3.5, units="in")


r <- ggplot(max, aes(x=tpcc_warehouse_count,y= r, color=factor(variant), group=factor(variant))) +
    geom_point(aes(shape=factor(variant)), size=4/.pt, alpha=0.5) +
    geom_line(size=1.75/.pt) +
    scale_shape_discrete(name=NULL, labels=labelByVariant, breaks=breakByVariant, guide =FALSE) +
    scale_color_manual(name =NULL, labels=labelByVariant, values=colorByVariant, breaks=breakByVariant, guide =FALSE) +
    labs(x='TPC-C Warehouses', y = 'Reads per transaction [KiB]') +
    theme_acm +
    scale_x_log10()+
    theme(legend.position = 'top') +
    expand_limits(y=0, x=0)
r

sqldf("select (min_gib * 1024.0/tpcc_warehouse_count) w_mib, min_gib, variant, tpcc_warehouse_count from max where min_gib > 240 group by variant, tpcc_warehouse_count order by tpcc_warehouse_count asc")

tmp=sqldf("select tpcc_warehouse_count, variant, median(r_mib/tx)*1024 r, median(w_mib/tx)*1024 w, median((w_mib+r_mib)/tx)*1024 io, max(tx) tx, sum(tx) total_tx, min(space_usage_gib) min_gib, max(space_usage_gib) max_gib from d where t>=300 group by tpcc_warehouse_count, variant")
tmp
kib_tx = sqldf("select ((max_gib - min_gib) * 1024 * 1024.0 / total_tx) kib, total_tx, min_gib, max_gib - min_gib, variant, tpcc_warehouse_count from tmp  group by variant, tpcc_warehouse_count order by tpcc_warehouse_count asc")
kib_tx

mib_w=sqldf("select min_gib/tpcc_warehouse_count,tpcc_warehouse_count,variant from max group by tpcc_warehouse_count,variant")

ggplot(kib_tx, aes(tpcc_warehouse_count, kib, color=factor(variant))) +geom_line()

d=read.csv('./C_wdc_stats.csv')
dts=read.csv('./C_wdc_dts.csv')
d=sqldf("select * from d")
dts=sqldf("select dts.*,d.c_cool_pct from dts, d where d.c_hash =dts.c_hash and d.t=1 ")
ggplot(dts, aes(t,dt_misses_counter )) + geom_line() + facet_grid (dt_name ~ c_cool_pct ) + scale_y_log10()

ggplot(d, aes(t,tx )) + geom_line() + facet_grid(c_cool_pct ~ tpcc_warehouse_count) + expand_limits(x=0, y=0)

d=read.csv('./C_full_stats.csv')

#d=sqldf("select * from d where c_cm_split=false and c_su_merge=false")
ggplot(d, aes(t,tx, color=interaction(c_cm_split, c_su_merge))) + geom_line() + facet_grid (. ~ tpcc_warehouse_count)

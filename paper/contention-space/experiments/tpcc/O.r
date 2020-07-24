source("../common.r", local = TRUE)
setwd("../tpcc")
# TPC-C Out of memory (O)

dev.set(0)
df=read.csv('./O_long_stats.csv')
df=sqldf("select * from df where t >0 and tpcc_warehouse_count=10000 and c_pp_threads=4")
df=sqldf(c("update df set c_cm_split=0", "select * from main.df"))
d= sqldf("
select *, 1 as symbol from df where c_su_merge=0 and c_cm_split=0
UNION select *, 2 as symbol from df where c_su_merge=0 and c_cm_split=1
UNION select *, 3 as symbol from df where c_su_merge=1 and c_cm_split=0
UNION select *, 4 as symbol from df where c_su_merge=1 and c_cm_split=1
")


d=read.csv('./O.csv')
d$tx <- d$tx/1e6
dev.set(0)
g <- ggplot(d, aes(t, tx, color=factor(symbol), group=factor(symbol))) +
    geom_point(aes(shape=factor(symbol)), alpha=0.5, size = 1/.pt) +
    scale_size_identity(name=NULL) +
    labs(x='Time [sec]', y = 'TPC-C throughput [M txns/sec]') +
    geom_smooth(method ="auto", se=FALSE, size=1.75/.pt) +
    scale_color_manual(guide=FALSE, breaks=c(1,3), values=c("black", "#619CFF"))+
    scale_shape_discrete(guide=FALSE)+
    theme_acm +
    theme(axis.title.y = element_text(hjust = 1.0)) +
    expand_limits(x=0,y=0) +
    annotate("text", x=1450, y=30000/1e6, label="Baseline", color ="black", size =7/.pt) +
    annotate("text", x=1450, y=55000/1e6, label="+XMerge", color = "#619CFF", size = 7/.pt)
g
ggsave('../../tex/figures/tpcc_O.pdf', width=lineWidthInInches , height = 1.75, units="in")

ggplot(d, aes (t, (w_mib)/tx, color=factor(symbol))) + geom_smooth() + expand_limits(y=0) + facet_grid(rows=vars(tpcc_warehouse_count))
dev.new()
ggplot(d, aes (t, (r_mib)/tx, color=factor(symbol))) + geom_smooth() + expand_limits(y=0) + facet_grid(rows=vars(tpcc_warehouse_count))


stats=read.csv('./O_long_stats.csv')
dts=read.csv('./O_long_dts.csv')

dts=sqldf("select stats.c_su_merge, dts.* from dts, stats where stats.tpcc_warehouse_count=10000 and stats.c_pp_threads=4 and dts.c_hash=stats.c_hash and stats.t=1 ")

ggplot(dts[dts$dt_name=='stock', ], aes(t, dt_misses_counter)) + facet_grid(col=vars(c_su_merge), row=vars(dt_name)) + geom_line()

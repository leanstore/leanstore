library(ggplot2)
library(sqldf)
library(Cairo)
library(stringr)
library(scales)
# TPC-C C: 100 warehouses, 120 threads
# 4 combinations of enabled/disabled space/contention management
                                        # C_short_threads.csv C_new.csv have the latest data

dev.set(0)

#df=read.csv('./C_rome_1000.csv')
#df=read.csv('./C_rome_short.csv')
df=read.csv('./intel/C_intel_long.csv')
df=sqldf("select * from df where t > 0 ")
d=sqldf("select * from df where c_su_merge=false and c_cm_split=false")
ops =sqldf("
select t, r_mib, sum(cm_split_succ_counter) splits, sum(su_merge_full_counter) merges, 'blue' as color from d where dt_name ='warehouse' group by t
")

so <- ggplot(ops, aes(t, splits)) +
    geom_point(color='blue') +
    geom_line(color='blue') +
    expand_limits(y=0) +
    theme_bw() +
    labs(x='Time (seconds)', y = 'Contention Splits/second')
so

mo <- ggplot(ops, aes(t, merges)) +
    geom_point(color='blue') +
    geom_line(color='blue') +
    expand_limits(y=0) +
    theme_bw() +
    labs(x='Time (seconds)', y = 'Eviction Merges/second')
mo




df=read.csv("./C_rome_1000_long.csv")
tmp=sqldf("select t, dt_name, sum(dt_misses_counter) page_reads from df where t > 10   group by t, dt_name  order by t asc")
rio <- ggplot(tmp, aes(t, page_reads)) +
    geom_point() +
    scale_y_log10() +
    geom_line() +
    labs(y='Page Reads/second', x = 'Time (second)')+
    facet_grid(row=vars(dt_name))
rio


CairoPDF("./rio_fix.pdf", bg="transparent", height= 10, width=8)
print(rio)
dev.off()

plot(tmp$t,tmp$misses)

sqldf("select dt_name, sum(dt_misses_counter) misses from d where symbol=4 group by dt_name  order by misses desc limit 10")

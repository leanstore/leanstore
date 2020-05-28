library(ggplot2)
library(sqldf)
library(Cairo)
library(stringr)
library(scales)

dev.set(0)
df=read.csv('./C_tmp.csv')
d= sqldf("select *, 1 as symbol from df where c_worker_threads in (30,90) and c_cm_split =0 and c_su_merge=0 and t > 0 ")

outofmemory = sqldf("select symbol,c_worker_threads,c_su_merge,c_cm_split,tx, min(t) as t, space_usage_gib from d where space_usage_gib > c_dram_gib group by symbol,c_worker_threads order by t asc")

acc=sqldf("select t,c_worker_threads,tx,space_usage_gib,c_su_merge,c_cm_split,symbol,sum(tx/1.0/1e6) OVER (PARTITION BY symbol, c_worker_threads ORDER BY t ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as txacc from d where c_worker_threads group by t,tx, c_su_merge,c_cm_split, symbol,space_usage_gib,c_worker_threads order by t asc")

head(acc)
head(outofmemory)
exc=sqldf("select * from d where dt_name='warehouse'")

dev.set(1)
ggplot(exc, aes(t, r_mib , color=factor(c_worker_threads), group=factor(c_worker_threads))) + geom_line() +
    theme_bw() +
    expand_limits(y=0)

dev.set(2)
ggplot(exc, aes(t, tx, color=factor(c_worker_threads), group=factor(c_worker_threads))) +
    expand_limits(y=0) +
    geom_line() +
    theme_bw()

ggplot(exc, aes(t, dt_restarts_update_same_size/tx, color=factor(c_worker_threads), group=factor(c_worker_threads))) +
    geom_line() +
    theme_bw() +
    expand_limits(y=0)

ggplot(exc, aes(t, dt_researchy_5 * 100.0 /c_worker_threads/1e6, color=factor(c_worker_threads), group=factor(c_worker_threads))) +
    geom_line() +
    theme_bw() +
    expand_limits(y=0)

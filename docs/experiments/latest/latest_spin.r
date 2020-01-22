library(ggplot2)
library(sqldf)
library(Cairo)
library(stringr)
library(scales)
# Rome

dev.set(0)
df=read.csv('/home/adnan/rome/dev/leanstore/docs/experiments/latest/spin.csv')
d=sqldf("select * from df ")
tx <- ggplot(d, aes(t, tx, color=c_contention_management, group=c_contention_management)) + geom_line()
tx <- tx + facet_grid (row=vars(c_zipf_factor, latest_window_ms), cols=vars(c_worker_threads,c_dram_gib))
print(tx)

r =sqldf("select t,latest_window_ms, c_dram_gib, c_zipf_factor,c_worker_threads, sum(dt_researchy_0) s, sum(dt_restarts_update_same_size) re from d group by t, c_dram_gib, c_zipf_factor, latest_window_ms,c_worker_threads")
restarts <- ggplot(r, aes(t, s)) + geom_line()
restarts <- restarts + facet_grid (row=vars(c_zipf_factor,latest_window_ms), cols=vars(c_worker_threads, c_dram_gib))
print(restarts)

sqldf("select max(tx)/1e6, c_contention_management from d group by c_contention_management")
sqldf("select max(tx),c_zipf_factor,sum(dt_researchy_0), sum(dt_restarts_update_same_size),c_dram_gib from d group by c_dram_gib")


dev.set(1)
df=read.csv('/home/adnan/rome/dev/leanstore/docs/experiments/latest/results_2.csv')
d=sqldf("select * from df")
tx <- ggplot(d, aes(t, tx, color=c_contention_management, group=c_contention_management)) + geom_line()
tx + facet_grid (row=vars(latest_window_ms), cols=vars(c_dram_gib))


debug=sqldf("select * from df where latest_read_ratio=25 and latest_window_ms=10000")

splits_d = sqldf("select t, sum(dt_researchy_0) s from d where c_contention_management =1 group by t")
ggplot(splits_d, aes(t, s)) + geom_line()


# Interesting
dev.set(0)
df=read.csv('/home/adnan/rome/dev/leanstore/docs/experiments/latest/interesting.csv')
d=sqldf("select * from df")
tx <- ggplot(d, aes(t, tx, color=c_contention_management, group=c_contention_management)) + geom_line()
tx <- tx + facet_grid (row=vars(latest_window_ms), cols=vars(c_dram_gib))
print(tx)

r =sqldf("select t,latest_window_ms, c_dram_gib, c_zipf_factor, sum(dt_restarts_update_same_size) re from d group by t, c_dram_gib, c_zipf_factor, latest_window_ms")
restarts <- ggplot(r, aes(t, re)) + geom_line()
restarts <- restarts + facet_grid (row=vars(latest_window_ms), cols=vars(c_dram_gib))
print(restarts)

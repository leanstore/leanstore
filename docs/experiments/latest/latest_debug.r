library(ggplot2)
library(sqldf)
library(Cairo)
library(stringr)
library(scales)
# Rome

dev.set(0)
df=read.csv('/home/adnan/rome/dev/leanstore/docs/experiments/latest/results.csv')
d=sqldf("select * from df ")
tx <- ggplot(d, aes(t, tx, color=c_contention_management, group=c_contention_management)) + geom_line()
tx <- tx + facet_grid (row=vars(c_zipf_factor, c_backoff_strategy), cols=vars(c_worker_threads,c_dram_gib))
print(tx)

r =sqldf("select t,max(GHz) GHz, min(instr) instr, max(space_usage_gib) space_usage_gib, latest_window_ms,  c_backoff_strategy, c_dram_gib, c_zipf_factor,c_worker_threads, sum(dt_researchy_0) splits, sum(dt_restarts_update_same_size) re from d where c_contention_management = 1 group by t, c_dram_gib, c_zipf_factor, latest_window_ms,c_worker_threads, c_backoff_strategy")
aux <- ggplot(r, aes(t, space_usage_gib)) + geom_line()
aux <- restarts + facet_grid (row=vars(c_zipf_factor, latest_window_ms), cols=vars(c_worker_threads,c_dram_gib))
print(aux)

sqldf("select max(tx)/1e6, c_contention_management from d group by c_contention_management")
sqldf("select max(tx),c_zipf_factor,sum(dt_researchy_0), sum(dt_restarts_update_same_size),c_dram_gib from d group by c_dram_gib")

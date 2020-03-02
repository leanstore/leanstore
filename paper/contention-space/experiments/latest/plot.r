library(ggplot2)
library(sqldf)
library(Cairo)
library(stringr)
library(scales)
# Rome

dev.set(0)
df=read.csv('./results.csv')
d=sqldf("select * from df")
tx <- ggplot(d, aes(t, tx, color=factor(c_cm_split), group=factor(c_cm_split))) + geom_line()
tx <- tx + facet_grid (row=vars(c_zipf_factor, latest_read_ratio, c_dram_gib), cols=vars(latest_window_gib,latest_window_offset_gib,latest_window_ms,  c_target_gib)) + xlim(0,30)
print(tx)
sqldf("select c_su_merge,c_cm_split,sum(cm_split_succ_counter), max(consumed_pages) from d group by c_cm_split,c_su_merge")

aux =sqldf("select t, max(GHz) GHz, min(instr) instr,
 max(space_usage_gib) space_usage_gib,
 sum(dt_restarts_update_same_size) restarts,
 sum(cm_split_succ_counter) splits,
 sum(su_merge_full_counter) merge_succ,
 sum(su_merge_partial_counter) merge_fail,
c_cm_split,c_su_merge,
latest_window_ms,  c_backoff_strategy, c_dram_gib, c_zipf_factor, c_worker_threads,c_cm_update_tracker_pct from d where t > 10 group by t, c_dram_gib, c_zipf_factor, latest_window_ms,c_worker_threads, c_backoff_strategy,c_cm_update_tracker_pct, c_cm_split, c_su_merge")
head(d)
plot <- ggplot(aux, aes(t)) + geom_line(aes(y=splits), color="red") + geom_line(aes(y=merge_succ), colour="blue")  + geom_line(aes(y=merge_fail), colour="green")
#plot <- ggplot(aux, aes(t)) + geom_line(aes(y=space_usage_gib), color="red")
plot <- plot + facet_grid (row=vars(latest_window_ms, c_cm_split), cols=vars(c_cm_update_tracker_pct,c_dram_gib, c_su_merge))
print(plot)

                                        #+ geom_line(aes(y=merge_fail), color="green", linetype="dotted")
# + geom_line(aes(y=space_usage_gib), color="purple", linetype="longdash")
sqldf("select max(tx)/1e6, c_cm_split from d group by c_cm_split")
sqldf("select max(tx),c_zipf_factor,sum(dt_researchy_0), sum(dt_restarts_update_same_size),c_dram_gib from d group by c_dram_gib")

source("../common.r")
setwd("../space")
# Rome

dev.set(0)
df=read.csv('/home/adnan/dev/workspace/db/leanstore/release/frontend/tmp.csv')
d=sqldf("select * from df")
tx <- ggplot(d, aes(t, tx, color=c_su_merge, group=c_su_merge)) + geom_line()
tx <- tx + facet_grid (row=vars(c_zipf_factor, c_dram_gib), cols=vars( c_target_gib))
head(d)
print(tx)
sqldf("select  t,tx,unswizzled, cool_pct,cool_pct_should,evicted_mib, free_pct, p1_pct,p2_pct,p3_pct from df order by t desc limit 2")
aux =sqldf("select t, max(GHz) GHz, min(instr) instr,
 max(space_usage_gib) space_usage_gib,
 sum(dt_restarts_update_same_size) restarts,
 sum(dt_researchy_0) splits_succ,
 sum(dt_researchy_1) splits_fail,
 sum(dt_researchy_2) merge_succ,
 sum(dt_researchy_3) merge_fail,
 sum(dt_researchy_4) merge_fail2,
 sum(dt_researchy_5) merge_k_full,
 sum(dt_researchy_6) merge_k_partial,
 sum(dt_researchy_8) frag_merge,
 sum(dt_researchy_9) frag_skip,
c_cm_split,c_su_merge,
  c_backoff_strategy, c_dram_gib, c_zipf_factor, c_worker_threads,c_cm_update_tracker_pct from d  group by t, c_dram_gib, c_zipf_factor, c_worker_threads, c_backoff_strategy,c_cm_update_tracker_pct, c_cm_split, c_su_merge")
plot <- ggplot(aux, aes(t)) + geom_line(aes(y=splits_succ), color="red") + geom_line(aes(y=merge_succ), colour="blue")  + geom_line(aes(y=merge_fail), colour="green") + geom_line(aes(y=space_usage_gib * 1024), color="purple") +geom_line(aes(y=merge_k_full ), color="brown") + geom_line(aes(y=merge_k_partial ), color="black")  + geom_line(aes(y=frag_merge ), color="orange")  + geom_line(aes(y=frag_skip ), color="gray")
#plot <- ggplot(aux, aes(t))
plot <- plot + facet_grid (row=vars(c_cm_split), cols=vars(c_cm_update_tracker_pct,c_dram_gib, c_su_merge))
print(plot)

df=read.csv('/home/adnan/dev/workspace/db/leanstore/release/frontend/merge.csv')
after=sqldf("select * from df where flag = 1")
sqldf("select flag, avg(ff) from df group by flag")
sqldf("select i from after where ff= 1 limit 10")
sqldf("select count(*) from after where ff <=0.5")/sqldf("select count(*) from after")
d=sqldf("select * from df where i < 100")
ggplot(d, aes(i, ff)) + geom_line() + scale_x_discrete()  + facet_grid (row=vars(flag))

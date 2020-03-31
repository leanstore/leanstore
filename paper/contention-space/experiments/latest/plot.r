library(ggplot2)
library(sqldf)
library(Cairo)
library(stringr)
library(scales)
library(gtable)
library(grid)
# Rome

dev.set(0)
#df=read.csv('./results.csv')
#df=read.csv('./results_mutex.csv')
df=read.csv('./tmp.csv')
d=sqldf("select * from df where c_worker_threads in (10,30,60) ")

tx <- ggplot(d, aes(t, tx, color=factor(c_cm_split), group=factor(c_cm_split))) +
    geom_line() +
    theme_bw() +
    expand_limits(y=0) +
    scale_color_discrete(name =NULL, labels=c("base", "+ Contention Split"), breaks=c(0,1)) +
    labs(x='Time (seconds)', y = 'Updates/second')
print(tx)

merges =sqldf("
select t,  sum(cm_split_succ_counter) counter, 'split' as type  from d where c_cm_split = true group by t  having counter > 0
UNION
select t,  sum(su_merge_full_counter) counter, 'merge' as type  from d where c_cm_split = true group by t having counter > 0
")
ops <- ggplot(merges, aes(t, counter, color=factor(type))) +
    geom_point() +
    scale_color_discrete(name =NULL, labels=c("+ Contention Splits", "+ Eviction Merges"), breaks=c('split','merge')) +
    expand_limits(y=0) +
    theme_bw() +
    labs(x='Time (seconds)', y = 'Operations/second')
print(ops)

g2 <- ggplotGrob(tx)
g3 <- ggplotGrob(ops)
g <- rbind(g2, g3, size = "first")
g$widths <- unit.pmax(g2$widths, g3$widths)
grid.newpage()
grid.draw(g)


CairoPDF("./latest.pdf", bg="transparent")
print(tx)
dev.off()


sqldf("select c_worker_threads,c_su_merge,c_cm_split,sum(cm_split_succ_counter), max(consumed_pages) from d group by c_cm_split,c_su_merge,c_worker_threads")

aux =sqldf("select t, max(GHz) GHz, min(instr) instr,
 max(space_usage_gib) space_usage_gib,
 sum(dt_restarts_update_same_size) restarts,
 sum(cm_split_succ_counter) splits,
 sum(su_merge_full_counter) merge_succ,
 sum(su_merge_partial_counter) merge_fail,
c_cm_split,c_su_merge,
latest_window_ms,  c_backoff_strategy, c_dram_gib, c_zipf_factor, c_worker_threads,c_cm_update_tracker_pct from d where t > 0 group by t, c_dram_gib, c_zipf_factor, latest_window_ms,c_worker_threads, c_backoff_strategy,c_cm_update_tracker_pct, c_cm_split, c_su_merge")
head(aux)

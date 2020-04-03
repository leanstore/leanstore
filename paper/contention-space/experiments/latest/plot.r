library(ggplot2)
library(sqldf)
library(Cairo)
library(stringr)
library(scales)
library(gtable)
library(grid)
# Rome

dev.set(0)
df=read.csv('./results_5.csv')
#df=read.csv('./results_mutex.csv')
#df=read.csv('./tmp.csv')
d=sqldf("select * from df where c_zipf_factor=0.9 and t <= 60")

tx <- ggplot(d, aes(t, tx, color=factor(c_cm_split), group=factor(c_cm_split))) +
    geom_point() +
    geom_line() +
    theme_bw() +
    expand_limits(y=0) +
    scale_color_discrete(name =NULL, labels=c("Base", "+ Contention Split"), breaks=c(0,1)) +
    theme(legend.position = 'top') +
#    facet_grid(rows=vars(c_zipf_factor))+
    labs(x='Time (seconds)', y = 'Updates/second')
tx

ops =sqldf("
select t,  sum(cm_split_succ_counter) splits, sum(su_merge_full_counter) merges, 'blue' as color from d where c_cm_split = true group by t
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
g2 <- ggplotGrob(tx)
g3 <- ggplotGrob(so)
g4 <- ggplotGrob(mo)
g <- rbind(g2, g3,g4,  size = "first")
g$widths <- unit.pmax(g2$widths, g3$widths, g4$widths)
grid.newpage()
grid.draw(g)
ggsave(file="latest.pdf", g)


CairoPDF("./latest.pdf", bg="transparent")
print(tata)
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

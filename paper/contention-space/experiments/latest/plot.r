source("../common.r")

dev.set(0)

stats=read.csv('./latest_stats.csv')
dts=read.csv('./latest_dts.csv')

tx <- ggplot(stats, aes(t, tx, color=factor(c_cm_split), group=factor(c_cm_split))) +
    geom_point() +
    geom_line() +
    expand_limits(y=0, x=0) +
    scale_color_manual(name =NULL, labels=c("Base", "+ Contention Split"), breaks=c(0,1), values=c("black", "red")) +
#    scale_color_discrete(name =NULL, labels=c("Base", "+ Contention Split"), breaks=c(0,1)) +
    theme(legend.position = 'top') +
    facet_grid(rows=vars(latest_window_offset_gib))+
    labs(x='Time [sec]', y = 'Updates/second')
tx

so <- ggplot(dts, aes(t, cm_split_succ_counter)) +
    geom_point(color='red') +
    geom_line(color='red') +
    expand_limits(y=0, x=0) +
    facet_grid(rows=vars(latest_window_offset_gib))+
    labs(x='Time [sec]', y = 'Contention Splits/second')
so

mo <- ggplot(dts, aes(t, su_merge_full_counter)) +
    geom_point(color='red') +
    geom_line(color='red') +
    expand_limits(y=0, x=0) +
    facet_grid(rows=vars(latest_window_offset_gib))+
    labs(x='Time [sec]', y = 'Eviction Merges/second')
mo

g2 <- ggplotGrob(tx)
g3 <- ggplotGrob(so)
g4 <- ggplotGrob(mo)
g <- rbind(g2, g3,g4,  size = "first")
g$widths <- unit.pmax(g2$widths, g3$widths, g4$widths)
grid.newpage()
grid.draw(g)
ggsave(file="latest.pdf", g, units=c("cm"), height=35)

io <- ggplot(stats, aes(t,r_mib, color=factor(c_cm_split), group=factor(c_cm_split))) +
    geom_point() +
    scale_y_log10() +
    geom_line()
io


CairoPDF("./latest.pdf", bg="transparent")
print(g)
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

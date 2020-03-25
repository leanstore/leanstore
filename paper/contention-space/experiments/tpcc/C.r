library(ggplot2)
library(sqldf)
library(Cairo)
library(stringr)
library(scales)
# TPC-C C: 100 warehouses, 120 threads
# 4 combinations of enabled/disabled space/contention management
                                        # C_short_threads.csv C_new.csv have the latest data

dev.set(0)

df=read.csv('./C_mutex.csv')
#d=sqldf("select * from d where t <=507")
d= sqldf("
select *, 1 as symbol from df where c_su_merge=0 and c_cm_split=0
UNION select *, 2 as symbol from df where c_su_merge=0 and c_cm_split=1
UNION select *, 3 as symbol from df where c_su_merge=1 and c_cm_split=0
UNION select *, 4 as symbol from df where c_su_merge=1 and c_cm_split=1
")
#outofmemory = sqldf("select symbol,c_su_merge,c_cm_split,tx, min(t) as t, space_usage_gib from d where space_usage_gib > c_dram_gib group by symbol order by t asc")
acc=sqldf("select t,c_worker_threads,tx,space_usage_gib,c_su_merge,c_cm_split,symbol,sum(tx/1.0/1e6) OVER (PARTITION BY symbol, c_worker_threads ORDER BY t ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as txacc from d where c_worker_threads group by t,tx, c_su_merge,c_cm_split, symbol,space_usage_gib,c_worker_threads order by t asc")
head(acc)

#tx <- ggplot(acc, aes(txacc, tx, color=factor(symbol), group=interaction(c_su_merge,c_cm_split))) +
tx <- ggplot(acc, aes(txacc, tx, color=factor(symbol), group=factor(symbol))) +
    geom_point(aes(shape=factor(symbol)), size=1, alpha=0.5) +
    scale_size_identity(name=NULL)+
    scale_shape_discrete(name=NULL, breaks=c(1,2,3,4), labels=c("base", "+CS -EM","-CS +EM","+CS +EM")) +
    scale_color_discrete(name =NULL, labels=c("base", "+CS -EM","-CS +EM","+CS +EM"), breaks=c(1,2,3,4)) +
    labs(x='Processed M Transactions [txn]', y = 'TPC-C throughput [txns/sec]') +
    geom_smooth() +
    theme_bw() +
   facet_grid(row=vars(c_worker_threads))#geom_point(data=outofmemory, aes(x=t,y=tx, colour=factor(symbol)), shape =4, size= 10)
print(tx)


## head(d)

## tx <- ggplot(d, aes(t, tx, color=factor(symbol), group=interaction(c_su_merge,c_cm_split))) +  geom_point(aes(shape=factor(symbol), size=1), alpha=0.05) + scale_size_identity(name=NULL)+ scale_shape_discrete(name=NULL, breaks=c(1,2,3,4), labels=c("base", "+split -merge","-split +merge","+split +merge")) + scale_color_discrete(name =NULL, labels=c("base", "+split -merge","-split +merge","+split +merge"), breaks=c(1,2,3,4)) + labs(x='time [sec]', y = 'TPC-C throughput [txns/sec]')  + geom_smooth() + theme_bw()
## print(tx)

## aux =sqldf("select t, max(GHz) GHz, min(instr) instr,symbol,
##  max(space_usage_gib) space_usage_gib,
##  sum(dt_restarts_update_same_size) restarts,
##  sum(cm_split_succ_counter) splits,
##  sum(su_merge_full_counter) merge_succ,
##  sum(su_merge_partial_counter) merge_fail,
## c_cm_split,c_su_merge,  c_backoff_strategy, c_dram_gib, c_zipf_factor, c_worker_threads,c_cm_update_tracker_pct from d group by t, symbol, c_dram_gib, c_zipf_factor, c_worker_threads, c_backoff_strategy,c_cm_update_tracker_pct, c_cm_split, c_su_merge")
## plot <- ggplot(aux, aes(t)) +
##     geom_line(aes(y=splits), color="red")+
##     #geom_line(aes(y=merge_succ), colour="blue") +
##     #geom_line(aes(y=merge_fail), colour="green") +
##     facet_grid (row=vars(c_su_merge), col=vars(c_cm_split))
## print(plot)

CairoPDF("./tpcc_C.pdf", bg="transparent")
print(tx)
dev.off()

library(ggplot2)
library(sqldf)
library(Cairo)
library(stringr)
library(scales)

dev.set(0)
skx=read.csv('./intel.csv')
#rome=read.csv('./F_tmp.csv')
rome=read.csv('./F_overnight.csv')
#rome=read.csv('./F_rome_without_smt.csv')
#rome=read.csv('./F_backoff.csv')
#rome=read.csv('./F_nocross.csv')
rome=sqldf("select *, '0' as [LLC-miss] from rome")

df = sqldf("select *, 'rome' as platform from rome UNION select *,'intel' as platform from skx")
df=sqldf("select * from df where dt_id=0")
affinity=sqldf("select platform,c_worker_threads,c_cm_split, tpcc_warehouse_affinity,max(tx) tx from df group by c_worker_threads, c_cm_split, tpcc_warehouse_affinity,platform")
ggplot(affinity, aes(x=c_worker_threads,y=tx, color=factor(tpcc_warehouse_affinity), group=factor(tpcc_warehouse_affinity)) ) +
    geom_line() +
    geom_point() +
    expand_limits(y=0) +
    facet_grid(row=vars(platform), col=vars(c_cm_split))


sqldf("select t, tx, instr, CPU, [L1.miss], [br.miss], median(IPC), c_worker_threads, tpcc_warehouse_affinity from rome where t = 10  group by c_worker_threads, tpcc_warehouse_affinity, tx")
sqldf("select sum(dt_restarts_update_same_size), c_worker_threads,dt_name, tpcc_warehouse_affinity from rome  where c_worker_threads = 44 group by c_worker_threads, tpcc_warehouse_affinity, dt_name order by dt_name asc")

test=sqldf("select median([instr]) miss, c_worker_threads,tpcc_warehouse_affinity from rome  where instr < 1e10 group by c_worker_threads,tpcc_warehouse_affinity")
ggplot(test,aes(x=c_worker_threads,y=miss, color=factor(tpcc_warehouse_affinity), group=factor(tpcc_warehouse_affinity))) + geom_point() + geom_line() + expand_limits(y=0)

test=sqldf("select avg([dt_restarts_update_same_size]) miss,c_worker_threads, c_cm_split,tpcc_warehouse_affinity from rome  where instr < 1e10 group by c_worker_threads,tpcc_warehouse_affinity, c_cm_split")
ggplot(test,aes(x=c_worker_threads,y=miss, color=factor(tpcc_warehouse_affinity), group=factor(tpcc_warehouse_affinity))) +
    geom_point() +
    geom_line() +
    expand_limits(y=0) +
    facet_grid(row=vars(c_cm_split))


backoffcsv=read.csv('./F_backoff.csv')
backoff=sqldf("select c_x,c_worker_threads,c_cm_split, tpcc_warehouse_affinity,max(tx) tx from backoffcsv  group by c_worker_threads, c_cm_split, tpcc_warehouse_affinity,c_x")
ggplot(backoff, aes(x=factor(c_worker_threads),y=tx, color=factor(tpcc_warehouse_affinity), group=factor(tpcc_warehouse_affinity)) ) +
    geom_line() +
    geom_point() +
    expand_limits(y=0) +
    facet_grid(row=vars(c_x), col=vars(c_cm_split))



pincsv=read.csv('./F_ccx.csv')
pin=sqldf("select tpcc_pin,c_worker_threads,c_cm_split, tpcc_warehouse_affinity,max(tx) tx from pincsv  group by c_worker_threads, c_cm_split, tpcc_warehouse_affinity,c_x, tpcc_pin")

ggplot(pin, aes(x=factor(c_worker_threads),y=tx, color=factor(tpcc_warehouse_affinity), group=factor(tpcc_warehouse_affinity)) ) +
    geom_line() +
    geom_point() +
    expand_limits(y=0) +
    facet_grid(row=vars(tpcc_pin), col=vars(c_cm_split))

restartcsv=read.csv('./F_restart.csv')
sqldf("select max(tx),tpcc_warehouse_affinity from restartcsv group by tpcc_warehouse_affinity")
sqldf("select dt_name,tpcc_warehouse_affinity, max([dt_restarts_update_same_size]) from restartcsv group by tpcc_warehouse_affinity,dt_name order by  dt_name")

sqldf("select tpcc_warehouse_affinity, sum([dt_restarts_update_same_size]) from backoffcsv where c_x=0 and c_worker_threads=90 group by tpcc_warehouse_affinity")

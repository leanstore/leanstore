library(ggplot2)
library(sqldf)

d=read.csv('rome_wo_smt.csv')
freq=sqldf("select t,c_pin_threads,c_worker_threads,c_mutex,c_backoff, count(*) freq  from d group by t,c_worker_threads,c_mutex,c_backoff,c_pin_threads")

sqldf("select c_pin_threads,c_worker_threads,c_mutex,c_backoff, stdev(freq) from freq group by c_worker_threads,c_mutex,c_backoff, c_pin_threads")


sqldf("select * from freq where c_worker_threads = 120 and c_mutex = false  and c_backoff = 0 order by freq desc")

sqldf("select sum(freq)/1048576.0,c_pin_threads, c_backoff,c_worker_threads from freq  where c_mutex = false and (t < 32 or ( t >= 64 and t < 96 ))  group by c_backoff,c_worker_threads, c_pin_threads")

sqldf("select sum(freq) from freq where c_worker_threads = 120 and c_mutex = false  and c_backoff = 512 ")


d=read.csv('../../../../release/frontend/fairness.csv')

freq=sqldf("select t,c_pin_threads,c_worker_threads,tag, count(*) as freq  from d group by t,c_worker_threads,c_pin_threads, tag")

ggplot(freq, aes(t,freq)) + facet_grid(rows=vars(c_mutex), cols=vars(c_worker_threads)) +
    geom_line()


ccx=sqldf("select (t / 16 ) as ccx, sum(freq) s,tag from freq group by tag, (t / 16) order by tag, s desc")
smallest=sqldf("select tag,min(s) as min from ccx group by tag")
sqldf("select *, s * 1.0 / smallest.min,smallest.tag from ccx, smallest where ccx.tag=smallest.tag ")

sqldf("select c_pin_threads,c_worker_threads,tag, stdev(freq) from freq where 1 or (t % 4) <> 0 group by c_worker_threads, c_pin_threads,tag")

sqldf("select c_hash, median(dt_researchy_9 * 100.0/(dt_researchy_8 + dt_researchy_9)) from dts group by c_hash")


d=read.csv('merged.csv')
sqldf("select * from d group by ")

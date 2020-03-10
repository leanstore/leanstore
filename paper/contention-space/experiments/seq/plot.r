library(ggplot2)
library(sqldf)

d=read.csv('rome.csv')

freq=sqldf("select t,c_worker_threads,c_mutex,c_backoff, count(*) freq  from d group by t,c_worker_threads,c_mutex,c_backoff")

sqldf("select c_worker_threads,c_mutex,c_backoff, stdev(freq) from freq group by c_worker_threads,c_mutex,c_backoff")


sqldf("select * from freq where c_worker_threads = 120 and c_mutex = false  and c_backoff = 0 order by freq desc")


sqldf("select sum(freq)/1048576.0, c_backoff from freq where c_worker_threads = 120 and c_mutex = false and (t < 32 or ( t >= 64 and t < 96 ))  group by c_backoff")

sqldf("select sum(freq) from freq where c_worker_threads = 120 and c_mutex = false  and c_backoff = 512 ")

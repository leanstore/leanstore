library(ggplot2)
library(sqldf)
# Rome
df=read.csv('/home/adnan/rome/dev/leanstore/docs/experiments/latest/latest_orig.csv')
d=sqldf("select * from df")
tx <- ggplot(d, aes(t, tx, color=c_contention_management, group=c_contention_management)) + geom_line()
tx + facet_grid (row=vars(latest_window_ms), cols=vars(latest_read_ratio))

debug=sqldf("select * from df where latest_read_ratio=25 and latest_window_ms=10000")
sqldf("select max(tx)/1e6, c_contention_management from debug group by c_contention_management")
sqldf("select sum(dt_researchy_0) from debug where c_contention_management=1")

splits_d = sqldf("select t, sum(dt_researchy_0) s from d where c_contention_management =1 group by t")
ggplot(splits_d, aes(t, s)) + geom_line()

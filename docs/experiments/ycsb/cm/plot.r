library(ggplot2)
library(sqldf)


# Rome
df=read.csv('/home/adnan/rome/dev/leanstore/docs/experiments/ycsb/cm/results.csv')
d=sqldf("select * from df")
tx <- ggplot(d, aes(t, tx, color=c_contention_management, group=c_contention_management)) + geom_line()
tx + facet_grid (row=vars(c_zipf_factor), cols=vars(ycsb_read_ratio))

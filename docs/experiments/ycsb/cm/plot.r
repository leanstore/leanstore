library(ggplot2)
library(sqldf)


# Rome
df=read.csv('/home/adnan/rome/dev/leanstore/docs/experiments/ycsb/cm/results.csv')
d=sqldf("select * from df")
tx <- ggplot(d, aes(t, tx, color=c_cm_split, group=c_cm_split)) + geom_line()
tx + facet_grid (row=vars(c_zipf_factor), cols=vars(ycsb_read_ratio))

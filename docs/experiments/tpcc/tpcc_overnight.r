library(ggplot2)
library(sqldf)

pcsv=read.csv('/home/adnan/rome/dev/leanstore/docs/experiments/tpcc/tpcc_overnight.csv')
ggplot(pcsv,aes(x=t, y=tx, color = c_cm_split, group=c_cm_split)) + geom_line() + facet_grid(row=vars(c_zipf_factor,tpcc_warehouse_count), cols=vars(c_worker_threads))

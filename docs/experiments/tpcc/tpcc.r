library(ggplot2)
library(sqldf)

# Rome

pcsv=read.csv('/home/adnan/rome/dev/leanstore/docs/experiments/tpcc/tpcc_60.csv')
ggplot(pcsv,aes(x=t, y=tx, color = c_contention_management)) + geom_line() + facet_grid(row=vars(tpcc_warehouse_count))


pcsv=read.csv('/home/adnan/rome/dev/leanstore/docs/analysis/csv/tpcc_100_skew.csv')
withcontention = sqldf("select * from pcsv where c = 1")
withoutcontention = sqldf("select * from pcsv where c = 0")
 ggplot() +
  geom_line(data =withcontention, aes(x = zipf, y = tx), color = "blue") +
  geom_line(data = withoutcontention, aes(x = zipf, y = tx), color = "red") +
  xlab('Skew') +
  ylab('TX') +  expand_limits(y = 0)

print(p)

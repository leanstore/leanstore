library(ggplot2)
library(sqldf)

# Rome
d=read.csv('/home/adnan/rome/dev/leanstore/docs/analysis/csv/tpcc/pp_tpcc_skew_100_0.99_true.csv')
sqldf("select max(tx) from d")


pcsv=read.csv('/home/adnan/rome/dev/leanstore/docs/analysis/csv/tpcc_60.csv')
withcontention = sqldf("select * from pcsv where c = 1")
withoutcontention = sqldf("select * from pcsv where c = 0")
 ggplot() +
  geom_line(data =withcontention, aes(x = w, y = tx), color = "blue") +
  geom_line(data = withoutcontention, aes(x = w, y = tx), color = "red") +
  xlab('Warehouses') +
  ylab('TX')

print(p)

pcsv=read.csv('/home/adnan/rome/dev/leanstore/docs/analysis/csv/tpcc_100_skew.csv')
withcontention = sqldf("select * from pcsv where c = 1")
withoutcontention = sqldf("select * from pcsv where c = 0")
 ggplot() +
  geom_line(data =withcontention, aes(x = w, y = tx), color = "blue") +
  geom_line(data = withoutcontention, aes(x = w, y = tx), color = "red") +
  xlab('Warehouses') +
  ylab('TX')

print(p)

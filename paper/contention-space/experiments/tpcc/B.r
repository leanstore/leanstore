library(ggplot2)
library(sqldf)
library(Cairo)
library(stringr)
library(scales)
# TPC-C B: 100 warehouses, 120 threads, zipf variable [In-memory]

dev.set(0)
df=read.csv('./B.csv')
d=sqldf("select c_zipf_factor, c_cm_split, max(tx) tx  from df group by c_cm_split, c_zipf_factor")

tx <- ggplot(d, aes(c_zipf_factor, tx, color=factor(c_cm_split), group=c_cm_split)) + geom_line()
tx <- tx + scale_x_continuous(name="Zipf Factor") + scale_y_continuous(name="TPC-C throughput [txn/s]") + scale_color_manual(name="", values=c("red", "blue"), labels=c("baseline", "+split +merge"))
print(tx)

CairoPDF("./tpcc_B.pdf", bg="transparent")
print(tx)
dev.off()

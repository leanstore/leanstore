library(ggplot2)
library(sqldf)
library(Cairo)
library(stringr)
library(scales)
# TPC-C C: 100 warehouses, 120 threads
# 4 combinations of enabled/disabled space/contention management

dev.set(0)
df=read.csv('./C.csv')
d= sqldf("
select *, 1 as symbol from df where c_su_merge=1 and c_cm_split=0
UNION select *, 3 as symbol from df where c_su_merge=0 and c_cm_split=1
UNION select *, 20 as symbol from df where c_su_merge=0 and c_cm_split=0
UNION select *, 10 as symbol from df where c_su_merge=1 and c_cm_split=1
")
tx <- ggplot(d, aes(t, tx, color=factor(symbol), group=interaction(c_su_merge,c_cm_split))) + geom_line() + geom_point(aes(shape=factor(symbol), size=1)) + scale_size_identity(name=NULL)+ scale_shape_discrete(name=NULL, breaks=c(1,3,20,10), labels=c("base", "+split -merge","-split +merge","+split +merge")) + scale_color_discrete(name =NULL, labels=c("base", "+split -merge","-split +merge","+split +merge"), breaks=c(1,3,20,10)) + labs(x='time [sec]', y = 'TPC-C throughput [txns/sec]')
print(tx)
head(d)

CairoPDF("./C.pdf", bg="transparent")
dev.off()

library(ggplot2)
library(sqldf)

d=read.csv('../cmake-build-release-g9/frontend/debug.csv')
d = sqldf("select * from d where f <=2")
d$prs = (d$poll / 100.0) * d$pr
d$pollms = (d$prs/1000.0)
plot(d$t, d$pollms)


plot(d$t, d$as)

plot(d$t, d$pc2/d$pc3)

plot(d$pr, d$cpus)

plot(d$t, d$pr)

plot(d$txs)
plot(d$wmibs/d$txs)
plot(d$rio/d$txs)

plot(d$t, d$rio)

plot(d$wmibs)

plot(d$uns)

plot(d$poll)

plot(d$cpus)

plot(d$p3)

plot(d$f)

plot(d$c)

plot(d$swi)

plot(d$uns)

plot(d$swi/d$as)

plot(d$as)

plot(d$swi, d$cpus)

plot(d$rio, d$cpus)

plot(d$poll,d$cpus)

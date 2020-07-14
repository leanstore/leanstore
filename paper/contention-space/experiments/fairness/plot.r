source("../common.r", local= TRUE)
setwd("../fairness")

# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
stats=read.csv('./fairness_stats.csv')
fairness=read.csv('./fairness.csv')

freq=sqldf("select f.t,s.c_hash,s.c_mutex, s.c_worker_threads,s.c_backoff, count(*) as freq from fairness f, stats s where f.c_hash=s.c_hash and s.t=1 group by f.t,s.c_hash")

ccx=sqldf("select (t / 4 ) as ccx, sum(freq) freq,c_mutex, c_backoff, c_hash from freq group by c_hash, (t / 4) order by c_hash, freq desc")
ccd=sqldf("select ccx/2 as ccd, sum(freq) freq, c_mutex, c_backoff, c_hash from ccx group by ccx/2, c_hash")

maxvalue=sqldf("select sum(freq) max from freq group by c_hash limit 1")
maxvalue

pct=sqldf("select c1.c_hash,c1.ccd, round(c1.freq * 100.0 /maxvalue.max, 1) spin, round(c2.freq * 100.0 /maxvalue.max, 1) mutex from ccd c1, ccd c2, maxvalue where c1.ccd=c2.ccd and c1.c_mutex=false and c2.c_mutex = true and c1.c_backoff=0 and c2.c_backoff=c1.c_backoff group by c1.c_hash, c1.ccd")
pct

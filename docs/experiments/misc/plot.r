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



w=read.csv('../cmake-build-release/frontend/workers.csv')

tables <- ggplot(w, aes(t, rio)) + geom_point()
tables + facet_grid (row=vars(name))


head(d)
plot(d$t, d$cpus)



d100=read.csv('/home/adnan/rome/dev/leanstore/release/frontend/pp_100.csv')
sqldf("select t,restarts_counter,tx,instr,allocate_ops from d100 where t > 10 limit 10")

d30=read.csv('/home/adnan/rome/dev/leanstore/release/frontend/pp_30.csv')
sqldf("select t,restarts_counter,tx,allocate_ops from d30 where t > 10 limit 10")


w1ac=read.csv('/home/adnan/rome/dev/leanstore/release/frontend/pp_1ac.csv')
w60ac=read.csv('/home/adnan/rome/dev/leanstore/release/frontend/pp_60ac.csv')
w120ac=read.csv('/home/adnan/rome/dev/leanstore/release/frontend/pp_120ac.csv')
w1=read.csv('/home/adnan/rome/dev/leanstore/release/frontend/pp_1.csv')
w30=read.csv('/home/adnan/rome/dev/leanstore/release/frontend/pp_30.csv')
#w10=read.csv('/home/adnan/rome/dev/leanstore/release/frontend/pp_10.csv')
w60=read.csv('/home/adnan/rome/dev/leanstore/release/frontend/pp_60.csv')
w120=read.csv('/home/adnan/rome/dev/leanstore/release/frontend/pp_120.csv')
w120a=read.csv('/home/adnan/rome/dev/leanstore/release/frontend/pp_120a.csv')
wtest=read.csv('/home/adnan/rome/dev/leanstore/release/frontend/pp_test.csv')
sqldf("select t,cool_pct,instr,cycle,cpus,GHz,\"L1.miss\",IPC, tx from w1 where tx > 10 limit 10")
sqldf("select t,cool_pct,instr,cycle,cpus,GHz,\"L1.miss\",IPC, tx from w30 where tx > 10 limit 10")
sqldf("select t,cool_pct,instr,cycle,cpus,GHz,\"L1.miss\",IPC, tx from w60 where tx > 10 limit 10")
sqldf("select t,cool_pct,instr,cycle,cpus,GHz,\"L1.miss\",IPC, tx from w120 where tx > 10 limit 10")
sqldf("select t,cool_pct,instr,cycle,cpus,GHz,\"L1.miss\",IPC, tx from w120a where tx > 10 limit 10")
sqldf("select t,cool_pct,instr,cycle,cpus,GHz,\"L1.miss\",IPC, tx from w1ac where tx > 10 limit 10")
sqldf("select t,cool_pct,instr,cycle,cpus,GHz,\"L1.miss\",IPC, tx from w60ac where tx > 10 limit 10")
sqldf("select t,cool_pct,instr,cycle,cpus,GHz,\"L1.miss\",IPC, tx from w120ac where tx > 10 limit 10")
sqldf("select t,cool_pct,instr,cycle,cpus,GHz,\"L1.miss\",IPC, tx from wtest where tx > 10 limit 10")

sqldf("select t,instr,cycle,cpus,GHz,\"L1.miss\",IPC, tx from w30 where tx > 10 limit 10")
sqldf("select t,instr,cycle,cpus,GHz,\"L1.miss\",IPC, tx from w60 where tx > 10 limit 10")
sqldf("select t,instr,cycle,cpus,GHz,\"L1.miss\",IPC, tx from w100 where tx > 10 limit 10")
#sqldf("select name,restarts_read r, restarts_modify m from w order by name ")


d=read.csv('/home/adnan/rome/dev/leanstore/release/frontend/dt_120ac.csv')
sqldf("select t,  sum(restarts_read) rr, sum(restarts_modify) rw from d group by t order by rr desc")
sqldf("select d.t,w.cool_pct,w.instr,w.cycle,w.cpus,w.GHz,w.\"L1.miss\",w.IPC, w.tx, w.allocate_ops, sum(d.restarts_modify), sum(d.restarts_read) from w120ac w, d  where tx > 10 and d.t=w.t group by d.t limit 400")

sqldf("select t, name, restarts_read rr, restarts_modify rw from d ")

ggplot(w, aes(t, restarts_read)) + geom_point() + facet_grid(row=vars(name))

pp=read.csv('/home/adnan/rome/dev/leanstore/release/frontend/pp_rio_light.csv')
w=read.csv('/home/adnan/rome/dev/leanstore/release/frontend/workers_rio_light.csv')
sqldf("select name,restarts_read, restarts_modify from w order by name ")
sqldf("select t,evicted_mib,r_mib,free_pct,cool_pct from pp order by t desc limit 20")



# Skylake
w1=read.csv('/home/adnan/dev/workspace/db/leanstore/cmake-build-release-g9/frontend/pp_1.csv')
w10=read.csv('/home/adnan/dev/workspace/db/leanstore/cmake-build-release-g9/frontend/pp_10.csv')
sqldf("select t,cool_pct,instr,cycle,cpus,GHz,\"L1.miss\",IPC, tx from w1 where tx > 10 limit 10")
sqldf("select t,cool_pct,instr,cycle,cpus,GHz,\"L1.miss\",IPC, tx from w10 where tx > 10 limit 10")


# Rome
d=read.csv('/home/adnan/rome/dev/leanstore/release/frontend/pp_verify.csv')
sqldf("select t,tx,unswizzled,free_pct,r_mib,evicted_mib,CPU,p1_pct,find_parent_pct,iterate_children_pct,p2_pct,p3_pct from d where tx > 0 order by t desc limit 10")
sqldf("select max(tx) from d")
d=read.csv('/home/adnan/rome/dev/leanstore/release/frontend/dt_verify.csv')
sqldf("select * from d order by t desc limit 10")


# YCSB - Contention
d=read.csv('/home/adnan/rome/dev/leanstore/release/frontend/pp_ycsb_20.csv')
sqldf("select t,tx,unswizzled,free_pct,r_mib,evicted_mib,CPU,p1_pct,find_parent_pct,iterate_children_pct,p2_pct,p3_pct from d where tx > 0 order by t asc limit 10")
sqldf("select max(tx) from d")
d=read.csv('/home/adnan/rome/dev/leanstore/release/frontend/dt_ycsb_20.csv')
sqldf("select * from d order by t desc limit 10")

d=read.csv('/home/adnan/rome/dev/leanstore/release/frontend/pp_ycsb_120.csv')
sqldf("select t,tx,unswizzled,free_pct,r_mib,evicted_mib,CPU,p1_pct,find_parent_pct,iterate_children_pct,p2_pct,p3_pct from d where tx > 0 order by t asc limit 10")
sqldf("select max(tx) from d")
d=read.csv('/home/adnan/rome/dev/leanstore/release/frontend/dt_ycsb_120.csv')
sqldf("select * from d order by t desc limit 10")








# Rome 120
d=read.csv('/home/adnan/rome/dev/leanstore/release/frontend/pp_120ac.csv')
sqldf("select t,tx, free_pct from d")

# Skx 20
d=read.csv('/home/adnan/dev/workspace/db/leanstore/cmake-build-release-g9/frontend/pp_20.csv')
sqldf("select t,tx from d")

dt=read.csv('/home/adnan/dev/workspace/db/leanstore/cmake-build-release-g9/frontend/dt_jmu.csv')
pp=read.csv('/home/adnan/dev/workspace/db/leanstore/cmake-build-release-g9/frontend/pp_jmu.csv')
sqldf("select name, sum(restarts_read), sum(restarts_updates), sum(restarts_structural) from dt group by name")
                                        #sqldf("select t,tx from pp")

# 484466

d=read.csv('/home/adnan/rome/dev/leanstore/release/frontend/pp_jmu.csv')
sqldf("select t,tx, free_pct from d")

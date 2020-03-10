library(ggplot2)
library(sqldf)


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
tail(pp$tx)
sqldf("select name, sum(restarts_read), sum(restarts_updates), sum(restarts_structural), sum(researchy) from dt group by name")
sqldf("select max(tx) from pp")

# Rome 120ac
dt=read.csv('/home/adnan/rome/dev/leanstore/release/frontend/dt_jmu.csv')
pp=read.csv('/home/adnan/rome/dev/leanstore/release/frontend/pp_jmu.csv')
tail(pp$tx)
tail(pp$cool_pct)
tail(pp$free_pct)
sqldf("select name, sum(restarts_read), sum(restarts_updates), sum(restarts_structural), sum(researchy_0), sum(researchy_1), sum(researchy_2) from dt group by name")
sqldf("select max(tx), min(tx), avg(tx), median(free_pct), min(free_pct), max(cool_pct) from pp")
plot(pp$t, pp$tx/1000)


fp = sqldf("select pp.t, pp.tx, dt.researchy from dt dt,pp pp  where pp.t=dt.t and name = 'warehouse' ")
plot(fp$t,fp$researchy)

tables <- ggplot(dt, aes(t, researchy_0)) + geom_point()
tables + facet_grid (row=vars(name))

sqldf("select name, sum(restarts_read), sum(restarts_updates), sum(restarts_structural), (researchy) from dt where t > 10 group by name")


# 484466

d=read.csv('/home/adnan/rome/dev/leanstore/release/frontend/pp_jmu.csv')
sqldf("select t,tx, free_pct from d")


plot(pp$t, pp$)

plot(pp$t, pp$p1_pct)

plot(pp$t, pp$p2_pct)

plot(pp$t, pp$p3_pct)

plot(pp$t, pp$submit_ms)



zipf=read.csv('/home/adnan/rome/dev/leanstore/release/frontend/zipf.csv')
ggplot(zipf, aes(x=k)) +
 geom_histogram(aes(y=..density..), colour="black", fill="white")+
 geom_density(alpha=.2, fill="#FF6666")
sqldf("select k, count(*) as freq from zipf group by k order by freq desc limit 20")

filter=sqldf("select * from zipf where i < 100")
ggplot(filter, aes(x=k)) +
    geom_histogram(color="black", fill="white")

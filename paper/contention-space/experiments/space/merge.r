source("../common.r", local = TRUE)
setwd("../space")


# raw: 3,8 + 0,5 = 4,3 GiB B+: 4,5 GiB, B+-EM: 3,5 GiB
# 72701109 lines --> 0,5 GiB u64 payloads

path <- './small'
joined <- joinResults(path)
ff=read.csv.sql(paste(c(path, '_merge.csv'), collapse=''))

sqldf("select f.flag, j.c_su_target_pct, avg(f.ff), median(f.ff), max(j.space_usage_gib) gib from joined j, ff f where f.c_hash=j.c_hash  group by j.c_su_target_pct, f.flag")

config=sqldf("select c_tag,c_hash, c_su_kwaymerge, min(space_usage_gib), gib from joined group by c_hash, c_su_kwaymerge")

sqldf("select c_su_kwaymerge,gib,j.c_hash,tag,flag, avg(ff), min(ff), median(ff) from ff f, config j where f.c_hash = j.c_hash and flag =1 group by tag, f.c_hash,flag, c_su_kwaymerge order by tag asc, gib desc")

sqldf("select c_hash, t, 100.0 *  su_merge_full_counter /dt_researchy_1, dt_researchy_1, dt_researchy_2, dt_researchy_3, dt_researchy_4,dt_researchy_5, su_merge_full_counter, su_merge_partial_counter from joined where su_merge_full_counter > 0 group by c_hash, t")

imp=sqldf("select c_tag,c_su_target_pct, t,CPU, space_usage_gib from joined where name ='merge'  group by t,c_su_target_pct order by  c_su_target_pct,t")
imp

sqldf("select sum(cycle)/72701109 from imp where space_usage_gib >=3.48")

sqldf("select c_tag, sum(cycle)/198841078 from imp where space_usage_gib >=26 group by c_tag")




hist(ff$ff)

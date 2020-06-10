library(ggplot2)
library(sqldf)
library(Cairo)
library(stringr)
library(scales)
# TPC-C A: 100 warehouse, in-memory, variable threads, with/-out split&merge

dev.set(0)
ff=read.csv('./em_a_merge.csv')
cpu=read.csv('./em_a_threads.csv')
stats=read.csv('./em_a_stats.csv')
dts=read.csv('./em_a_dts.csv')
sqldf("select t, dt_researchy_1, dt_researchy_2, dt_researchy_3, dt_researchy_4,dt_researchy_5, su_merge_full_counter, su_merge_partial_counter from dts where su_merge_full_counter > 0")

sqldf("select avg(ff), min(ff), max(ff), flag from df group by flag")

sqldf("select c.*, s.space_usage_gib from cpu c, stats s where name ='merge' and instr > 0 and s.t=c.t")

# raw: 3,8 + 0,5 = 4,3 GiB B+: 4,5 GiB, B+-EM: 3,5 GiB
# 72701109 lines --> 0,5 GiB u64 payloads

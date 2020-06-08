library(ggplot2)
library(sqldf)
library(Cairo)
library(stringr)
library(scales)
# TPC-C A: 100 warehouse, in-memory, variable threads, with/-out split&merge

dev.set(0)
ff=read.csv('./em_a_merge.csv')
cpu=read.csv('./em_a_threads.csv')
tx=read.csv('./em_a_stats.csv')

sqldf("select avg(ff), min(ff), max(ff), flag from df group by flag")

sqldf("select * from cpu where name ='merge' and instr > 0 limit 10")

# raw: 3,8 + 0,5 = 4,3 GiB B+: 4,5 GiB, B+-EM: 3,5 GiB
# 72701109 lines --> 0,5 GiB u64 payloads

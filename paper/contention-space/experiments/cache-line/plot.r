library(ggplot2)
library(sqldf)
library(Cairo)
library(stringr)
library(scales)

df =read.csv("./data.csv")
d=sqldf("select * from df")
ggplot(d, aes(x=t, y=tx, color=factor(cl), shape=factor(cl), group=factor(cl)))+
    geom_point() +
    geom_line() +
#    scale_y_log10()+
    expand_limits(y=0, x=0) +
    scale_color_discrete(name = 'counters per cache line') +
    scale_shape_discrete(name = 'counters per cache line') +
    labs(x='Threads', y = 'Throughput [increments/second]')


ggplot(d, aes(x=cl, y=tx))+
    scale_x_reverse(breaks=c(1,2,4,8)) +
#    geom_hline(yintercept = 190083815 * 32, color ='purple', linetype="dashed") +
    geom_hline(yintercept = h, color ='red') +
    geom_point() +
    geom_line() +
    scale_y_log10() +
    geom_text(aes(x=4,y=h,label = 'perfect scaling with 64 threads', family='tahoma' ,vjust = -1, color='red', size=2, font='sans')) +
    expand_limits(y=0, x= 0) +
    labs(x='Counters in a cache line', y = 'Throughput [increments/second]') +
    theme(legend.position = "none")

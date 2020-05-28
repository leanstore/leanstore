library(ggplot2)
library(sqldf)
library(Cairo)
library(stringr)
library(scales)
library(gtable)
library(grid)
# TPC-C C: 100 warehouses, 120 threads
# 4 combinations of enabled/disabled space/contention management
                                        # C_short_threads.csv C_new.csv have the latest data

dev.set(0)

rome=read.csv('./C_rome_incomplete.csv')

rome=sqldf("select *, '' as 'LLC.miss', 'rome' as type from rome where c_cm_split=false and c_su_merge=false")

aws=read.csv('./intel/C_intel_long.csv')
aws=sqldf("select *, 'aws' as type from aws where c_cm_split=false and c_su_merge=false")

both=sqldf("select * from rome union select * from  aws")
both=sqldf("select * from both where t > 60 and t <= 300")

rm <- ggplot(both, aes(t, r_mib)) +
    geom_point(color='red') +
    geom_line(color='red') +
    expand_limits(y=0) +
    theme_bw() +
    facet_grid(row=vars(type)) +
    labs(x='Time (seconds)', y = 'Read IO (MiB/s)')
rm

wm <- ggplot(both, aes(t, w_mib)) +
    geom_point(color='blue') +
    geom_line(color='blue') +
    expand_limits(y=0) +
    theme_bw() +
    facet_grid(row=vars(type)) +
    labs(x='Time (seconds)', y = 'Page Provider Write (MiB/s)')
wm

g2 <- ggplotGrob(rm)
g3 <- ggplotGrob(wm)
g <- rbind(g2, g3,  size = "first")
g$widths <- unit.pmax(g2$widths, g3$widths)
grid.newpage()
grid.draw(g)
ggsave(file="io.pdf", g)

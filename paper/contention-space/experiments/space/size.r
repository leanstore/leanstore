source("../common.r", local = TRUE)
setwd("../space")


# bar diagram http://www.sthda.com/english/wiki/ggplot2-barplots-quick-start-guide-r-software-and-data-visualization#change-outline-colors
                                        # B+-Tree, X Merge
stats=read.csv('size_stats.csv')
max=sqldf("select s.c_tag, t, max(s.space_usage_gib) btree from stats s group by s.c_tag")
min=sqldf("select s.c_tag, s.t, min(s.space_usage_gib) xmerge from stats s, max m where s.c_tag = m.c_tag and s.t > m.t group by s.c_tag")
res=sqldf("select max.c_tag, max.btree, min.xmerge from max,min where max.c_tag = min.c_tag")
res=sqldf("
select  substr(c_tag,0, instr(c_tag, '-') ) as dataset, 'B-Tree' as type, c_tag, btree as size from res
UNION select  substr(c_tag,0, instr(c_tag, '-') ) as dataset, '+XMerge' as type,c_tag, xmerge as size from res
")
res

res=sqldf("select *, 'Ordered' as insertionorder from res where c_tag like '%seq%' union select *,'Shuffled' as insertionorder from res where c_tag like '%rnd%' ")
res=sqldf("select * from res order by dataset asc,size desc")
res

g <- ggplot(res, aes(label=size, x=reorder(type, -size), y=size)) +
    facet_grid(cols=vars(dataset), row=vars(insertionorder)) +
    scale_y_continuous(limit=c(0,12)) +
    geom_bar(stat="identity", position =  position_dodge())+
    geom_text(position=position_dodge(1), vjust=-0.5, size = 2) +
    theme(axis.text.x = element_text(angle = 45, vjust = 1, hjust=1), axis.title.x=element_blank()) +
    theme(strip.background = element_blank(), strip.text = element_text (size = 9)) +
    labs (y = "Size [GiB]")
g
ggsave('../../tex/figures/size.pdf', width=3 , height = 2, units="in")

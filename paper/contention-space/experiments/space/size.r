source("../common.r", local = TRUE)
setwd("../space")


# bar diagram http://www.sthda.com/english/wiki/ggplot2-barplots-quick-start-guide-r-software-and-data-visualization#change-outline-colors
                                        # B+-Tree, X Merge
stats=read.csv('size_stats.csv')
max=sqldf("select s.c_tag, t, max(s.space_usage_gib) btree from stats s group by s.c_tag")
min=sqldf("select s.c_tag, s.t, min(s.space_usage_gib) xmerge from stats s, max m where s.c_tag = m.c_tag and s.t>m.t group by s.c_tag")
res=sqldf("select max.c_tag, max.btree, min.xmerge from max,min where max.c_tag = min.c_tag")
res=sqldf("
select  substr(c_tag,0, instr(c_tag, '-') ) as dataset, 'B-Tree - ' || substr(c_tag, instr(c_tag, '-') + 1) as type, btree as size from res
UNION select  substr(c_tag,0, instr(c_tag, '-') ) as dataset, '+XMerge - ' || substr(c_tag, instr(c_tag, '-') + 1) astype, xmerge as size from res
")
res=sqldf("select * from res order by dataset asc,size desc")
res$type <- gsub("rnd", "Random", res$type)
res$type <- gsub("seq", "Sequential", res$type)

g <- ggplot(res, aes(label=size, x=reorder(type, -size), y=size)) +
    facet_grid(cols=vars(dataset), labeller = label_both) +
    geom_bar(stat="identity", position =  position_dodge())+
    geom_text(position=position_dodge(1), vjust=-0.5) +
    theme(axis.text.x = element_text(angle = 45, vjust = 1, hjust=1), axis.title.x=element_blank())

CairoPDF("./size.pdf", bg="transparent", height=6, width =10)
print(g)
dev.off()

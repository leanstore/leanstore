source("../common.r", local = TRUE)
setwd("../space")
library(reshape2)
library(plotly)

# bar diagram http://www.sthda.com/english/wiki/ggplot2-barplots-quick-start-guide-r-software-and-data-visualization#change-outline-colors
                                        # B+-Tree, X Merge
stats=read.csv('size_stats.csv')
max=sqldf("select s.c_tag, t, max(s.space_usage_gib) btree from stats s group by s.c_tag")
min=sqldf("select s.c_tag, s.t, min(s.space_usage_gib) xmerge from stats s, max m where s.c_tag = m.c_tag and s.t > m.t group by s.c_tag")
res=sqldf("select max.c_tag, max.btree, min.xmerge from max,min where max.c_tag = min.c_tag")
res=sqldf("
select  'B-Tree' as type, c_tag, btree as size from res
UNION select '+XMerge' as type,c_tag, xmerge as size from res
")
res=sqldf("select * from res where c_tag not like '%wiki%' order by c_tag asc")
res$c_tag <- as.character(res$c_tag)
res$c_tag [ res$c_tag == "emails-rnd"] <- "Random-Emails"
res$c_tag [ res$c_tag == "emails-seq"] <- "Sorted-Emails"
res$c_tag [ res$c_tag == "urls-rnd"] <- "Random-URLs"
res$c_tag [ res$c_tag == "urls-seq"] <- "Sorted-URLs"
res$c_tag [ res$c_tag == "wikititles2-rnd"] <- "Random-Wiki Titles"
res$c_tag [ res$c_tag == "wikititles2-seq"] <- "Sorted-Wiki Titles"


g <- ggplot(res, aes(label=size, x=reorder(type, -size), y=size)) +
    facet_wrap( ~ c_tag , scales="free_y", ncol = 4) +
    geom_bar(stat="identity", position =  position_dodge())+
    geom_text(position=position_dodge(1), vjust=1.5, size = 2, color ="white") +
    theme(axis.text.x = element_text(angle = 45, vjust = 1, hjust=1), axis.title.x=element_blank()) +
    theme(strip.background = element_blank(), strip.text = element_text (size = 6)) +
    labs (y = "Size [GiB]")
g
ggsave('../../tex/figures/size.pdf', width=3.3374, height = 1.5, units="in")

# c_tag: sorted urls, type: b-tree, size: 1GiB

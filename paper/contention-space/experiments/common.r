library(ggplot2)
library(sqldf)
library(Cairo)
#library(stringr)
library(scales)
library(gtable)
library(grid)

# color scheme: base black, contention split red, XMerge blue, both purple
# TODO: base_size
theme_adnan <- theme_bw() +
    theme(axis.title=element_text(size=14), axis.text=element_text(size=12))

theme_set(theme_adnan)

joinResults <- function(path) {
    ff=read.csv(paste(c(path, '_merge.csv'), collapse=''))
    colnames(ff) <- paste("f", colnames(ff), sep = "_")
    cpu=read.csv(paste(c(path, '_threads.csv'), collapse=''))
    colnames(cpu) <- paste("c", colnames(cpu), sep = "_")
    stats=read.csv(paste(c(path, '_stats.csv'), collapse=''))
    colnames(stats) <- paste("s", colnames(stats), sep = "_")
    dts=read.csv(paste(c(path, '_dts.csv'), collapse=''))
    colnames(dts) <- paste("d", colnames(dts), sep = "_")
    joined = sqldf("select * from cpu c, dts d, stats s where c.c_c_hash = d.d_c_hash and c.c_t=d.d_t and s.s_t=d.d_t and d.d_c_hash=s.s_c_hash")
    colnames(joined) <- substring(colnames(joined), first=3,last=10000)
    t <- joined$t
    final <- joined[, !duplicated(colnames(joined))]
    final$t <- t
    return(final)
}

breakByVariant <- c(1,2,3,4)
labelByVariant <- c("Baseline", "+Contention Split", "+XMerge", "+Contention Split +XMerge")
#colorByVariant <- c("black", "red","blue", "purple")
colorByVariant <- c("black", "#F8766D", "#619CFF", "purple")
# green: #00BA38

# write.csv(arm, './A/arm/arm_stats.csv', quote=FALSE, row.names = FALSE)

library(ggplot2)
library(sqldf)
library(Cairo)
library(stringr)
library(scales)
library(gtable)
library(grid)

# color scheme: base black, contention split red, XMerge blue, both purple
theme_adnan <- theme_bw() +
    theme(axis.title=element_text(size=14), axis.text=element_text(size=12))

theme_set(theme_adnan)

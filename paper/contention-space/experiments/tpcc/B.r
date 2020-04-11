library(ggplot2)
library(sqldf)
library(Cairo)
library(stringr)
library(scales)
# TPC-C B: 100 warehouses, 120 threads, zipf variable [In-memory]
# intel : c5.24xlarge

dev.set(0)
#df=read.csv('./B_intel.csv')
df=read.csv('./B_rome.csv')
d=sqldf("select c_worker_threads,c_zipf_factor, c_cm_split, max(tx) tx  from df where c_zipf_factor < 0.99 group by c_cm_split, c_zipf_factor,c_worker_threads")


tx <- ggplot(d, aes(c_zipf_factor, tx, color=factor(c_cm_split), group=c_cm_split)) +
    geom_line() +
    geom_point() +
    scale_x_continuous(name="Zipf Factor (Skew)") +
    scale_y_continuous(name="TPC-C throughput [txn/s]") +
    scale_color_manual(name="", values=c("red", "blue"), labels=c("Baseline", "+ Contention Split")) +
    expand_limits(y=0) +
    theme_bw() +
    facet_grid(row=vars(c_worker_threads))
print(tx)


baseline=sqldf("select max(tx) baseline from df where c_cm_split = false and c_zipf_factor = 0 and c_worker_threads=120")
baseline
relative=sqldf("select c_worker_threads,c_zipf_factor, c_cm_split, max(tx) * 1.0 / (1.0 * baseline) factor  from df, baseline  where c_zipf_factor < 0.99 group by c_cm_split, c_zipf_factor,c_worker_threads")
relative
ggplot(relative, aes(c_zipf_factor, factor, color=factor(c_cm_split), group=c_cm_split)) +
    geom_line() +
    geom_point() +
    scale_x_continuous(name="Zipf Factor (Skew)") +
    scale_y_continuous(name="Performance relative to baseline (skew = 0)") +
    scale_color_manual(name="", values=c("red", "blue"), labels=c("Baseline", "+ Contention Split")) +
    expand_limits(y=0) +
    theme_bw() +
    facet_grid(row=vars(c_worker_threads))


CairoPDF("./tpcc_B.pdf", bg="transparent", height = 3)
print(tx)
dev.off()

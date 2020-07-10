library(lattice)
library(plotrix)

source("parse.R")

# MEMORY USAGE
##############

# load memory usage data
tt_mem <- mem_usage_transform('trendingtopics/logs/memory.total.used.csv')
wc_mem <- mem_usage_transform('wordcount/logs/memory.total.used.csv')
lp_mem <- mem_usage_transform('logprocessing/logs/memory.total.used.csv')
sa_mem <- mem_usage_transform('sentimentanalysis/logs/memory.total.used.csv')
sd_mem <- mem_usage_transform('spikedetection/logs/memory.total.used.csv')
sf_mem <- mem_usage_transform('spamfilter/logs/memory.total.used.csv')
fd_mem <- mem_usage_transform('frauddetection/logs/memory.total.used.csv')
ca_mem <- mem_usage_transform('clickanalytics/logs/memory.total.used.csv')
mo_mem <- mem_usage_transform('machineoutlier/logs/memory.total.used.csv')
aa_mem <- mem_usage_transform('ads-analytics/logs/memory.total.used.csv')
bi_mem <- mem_usage_transform('bargain-index/logs/memory.total.used.csv')
rl_mem <- mem_usage_transform('reinforcement-learner/logs/memory.total.used.csv')
sg_mem <- mem_usage_transform('smart-grid/logs/memory.total.used.csv')
tm_mem <- mem_usage_transform('traffic-monitoring/logs/memory.total.used.csv')
vs_mem <- mem_usage_transform('voipstream/logs/memory.total.used.csv')

# plot density charts
pdf('summary/memory_usage.pdf')
xlim <- c(0,5500)
par(mfrow=c(4,4))
plot(density(sa_mem$value), xlim=xlim, main='Sentiment Analysis')
plot(density(tt_mem$value), xlim=xlim, main='Trending Topics')
plot(density(sd_mem$value), xlim=xlim, main='Spike Detection')

plot(density(lp_mem$value), xlim=xlim, main='Log Processing')
plot(density(wc_mem$value), xlim=xlim, main='Word Count')
plot(density(sf_mem$value), xlim=xlim, main='Spam Filter')

plot(density(fd_mem$value), xlim=xlim, main='Fraud Detection')
plot(density(ca_mem$value), xlim=xlim, main='Click Analytics')
plot(density(mo_mem$value), xlim=xlim, main='Machine Outlier')

plot(density(aa_mem$value), xlim=xlim, main='Ads Analytics')
plot(density(bi_mem$value), xlim=xlim, main='Bargain Index')
plot(density(rl_mem$value), xlim=xlim, main='Reinforcement Learner')

plot(density(sg_mem$value), xlim=xlim, main='Smart Grid')
plot(density(tm_mem$value), xlim=xlim, main='Traffic Monitoring')
plot(density(vs_mem$value), xlim=xlim, main='VoIPStream')
dev.off()


# SELECTIVITY
#############
selectivity <- read.csv('summary.csv', header=TRUE)

png('summary/selectivity.png', width=3000, height=1080, units="px")
dotchart(selectivity$ratio, labels=selectivity$operator, groups=selectivity$app, xlab="Ratio", xlim=c(0.0, 195), pch=16)
axis(side=1, at=seq(0, 195, by=5))
#axis.break(1, 75, style="slash")
dev.off()


# alternative
pdf('summary/selectivity_new.pdf')
dotchart(selectivity$ratio, groups=selectivity$application, xlab="Ratio", pch=16)
axis.break(1, 40, style="slash")
axis(side=1, at=seq(0, 30, by=1))
axis(side=1, at=seq(150, 195, by=1))
dev.off()


# PROCESS TIME
##############
data <- read.csv('summary.csv', header=TRUE)

png('summary/process_time.png', width=1000, height=800, units="px")
barchart(pt_99th~app, data=data, groups=data$operator_id, ylim=c(0, 0.68), ylab="Processing Time (ms)", xlab="Application's Operators")
dev.off()


png('summary/process_time_outliers.png', width=1000, height=300, units="px")
barchart(pt_99th~app, data=data, groups=data$operator_id, ylim=c(1430, 1435), ylab="", xlab="")
dev.off()

#pdf
pdf('summary/process_time.pdf', width=12, height=8)
barchart(pt_99th~app, data=data, groups=data$operator_id, ylim=c(0, 0.68), ylab="Processing Time (ms)", xlab="Application's Operators", cex=4.5)
dev.off()


pdf('summary/process_time_outliers.pdf', width=12, height=3)
barchart(pt_99th~app, data=data, groups=data$operator_id, ylim=c(1430, 1435), ylab="", xlab="")
dev.off()



# TUPLE SIZE
############
sf_size <- read_tuple_size('spamfilter/summary/tuple_size.csv')
fd_size <- read_tuple_size('frauddetection/summary/tuple_size.csv')
ca_size <- read_tuple_size('clickanalytics/summary/tuple_size.csv')
mo_size <- read_tuple_size('machineoutlier/summary/tuple_size.csv')
wc_size <- read_tuple_size('wordcount/summary/tuple_size.csv')
lp_size <- read_tuple_size('logprocessing/summary/tuple_size.csv')
sa_size <- read_tuple_size('sentimentanalysis/summary/tuple_size.csv')
sd_size <- read_tuple_size('spikedetection/summary/tuple_size.csv')
tt_size <- read_tuple_size('trendingtopics/summary/tuple_size.csv')
aa_size <- read_tuple_size('ads-analytics/summary/tuple_size.csv')
bi_size <- read_tuple_size('bargain-index/summary/tuple_size.csv')
rl_size <- read_tuple_size('reinforcement-learner/summary/tuple_size.csv')
sg_size <- read_tuple_size('smart-grid/summary/tuple_size.csv')
tm_size <- read_tuple_size('traffic-monitoring/summary/tuple_size.csv')
vs_size <- read_tuple_size('voipstream/summary/tuple_size.csv')

#png('summary/tuple_size.png', width=1000, height=1000, units="px")
pdf('summary/tuple_size.pdf', width=12, height=12)
par(mfrow=c(4,4))
boxplot(tuple_size ~ operator, data = sf_size, outline=FALSE, main='Spam Filter', ylab="Tuple Size (bytes)", xlab="Operator")
boxplot(tuple_size ~ operator, data = fd_size, outline=FALSE, main='Fraud Detection', ylab="Tuple Size (bytes)", xlab="Operator")
boxplot(tuple_size ~ operator, data = ca_size, outline=FALSE, main='Click Analytics', ylab="Tuple Size (bytes)", xlab="Operator")
boxplot(tuple_size ~ operator, data = mo_size, outline=FALSE, main='Machine Outlier', ylab="Tuple Size (bytes)", xlab="Operator")
boxplot(tuple_size ~ operator, data = wc_size, outline=FALSE, main='WordCount', ylab="Tuple Size (bytes)", xlab="Operator")
boxplot(tuple_size ~ operator, data = lp_size, outline=FALSE, main='Log Processing', ylab="Tuple Size (bytes)", xlab="Operator")
boxplot(tuple_size ~ operator, data = sa_size, outline=FALSE, main='Sentiment Analysis', ylab="Tuple Size (bytes)", xlab="Operator")
boxplot(tuple_size ~ operator, data = sd_size, outline=FALSE, main='Spike Detection', ylab="Tuple Size (bytes)", xlab="Operator")
boxplot(tuple_size ~ operator, data = tt_size, outline=FALSE, main='Trending Topics', ylab="Tuple Size (bytes)", xlab="Operator")
boxplot(tuple_size ~ operator, data = aa_size, outline=FALSE, main='Ads Analytics', ylab="Tuple Size (bytes)", xlab="Operator")
boxplot(tuple_size ~ operator, data = bi_size, outline=FALSE, main='Bargain Index', ylab="Tuple Size (bytes)", xlab="Operator")
boxplot(tuple_size ~ operator, data = rl_size, outline=FALSE, main='Reinforcement Learner', ylab="Tuple Size (bytes)", xlab="Operator")
boxplot(tuple_size ~ operator, data = sg_size, outline=FALSE, main='Smart Grid', ylab="Tuple Size (bytes)", xlab="Operator")
boxplot(tuple_size ~ operator, data = tm_size, outline=FALSE, main='Traffic Monitoring', ylab="Tuple Size (bytes)", xlab="Operator")
boxplot(tuple_size ~ operator, data = vs_size, outline=FALSE, main='VoIPStream', ylab="Tuple Size (bytes)", xlab="Operator")
dev.off()

# get quantile
qtn <- quantile(sf_size$tuple_size, probs=c(0.99))
# get range
rgn <- range(qtn)

boxplot(tuple_size ~ operator, data = sf_size, outline=FALSE, xlim=c(0,9), ylim=range(sf_size$tuple_size, fd_size$tuple_size), xaxt="n", at=1:4)
boxplot(tuple_size ~ operator, data = fd_size, outline=FALSE, xaxt="n", add=TRUE, at=1:4 + 4)

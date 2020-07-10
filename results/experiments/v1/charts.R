library(plyr)
library(ggplot2)
library(reshape2)



#-Resource usage ---------------------------------------------------------------
# read data
res_usage <- read.csv('output/resources.espbench-0.csv', header=TRUE, stringsAsFactors=FALSE)

errors <- read.csv('output/errors.espbench-0.csv', header=TRUE)

# plot cpu/memory usage + errors/warnings
ggplot(res_usage, aes(timestamp)) + 
    geom_line(aes(y=mem_used), colour='green') +
    geom_line(aes(y=cpu_used), colour='blue') +
    geom_point(aes(x=timestamp, y=-10, colour=factor(level)), data=errors) +
#    geom_text(aes(y=-10, label=message, size=.5), data=errors)
    
# network usage
ggplot(res_usage, aes(timestamp)) + 
    geom_line(aes(y=net_recv), colour='green') +
    geom_line(aes(y=net_sent), colour='blue')
    




#-Latency ----------------------------------------------------------------------
latency <- read.csv('output/latency.csv', header=TRUE, stringsAsFactors=FALSE)
latency_m = melt(latency, id.vars='timestamp')
latency_mf <- latency_m[latency_m$variable != "samples", ]

# timeline
ggplot(latency, aes(timestamp)) + 
    geom_line(aes(y=p99_max), colour='red') +
    geom_line(aes(y=mean), colour='green') +
    geom_line(aes(y=p999_min), colour='blue')
    
# all values
ggplot(latency_mf, aes(x=timestamp, y=value, color=variable)) + 
    geom_line()

# histogram
hist(latency$p99_max, breaks=100)


#-Throughput -------------------------------------------------------------------
tp <- read.csv('output/throughput.csv', header=TRUE, stringsAsFactors=FALSE)
tp_m = melt(tp, id.vars='timestamp')

# timeline comparison
ggplot(tp_m, aes(x=timestamp, y=value, color=variable)) + 
    geom_line()
    
# histogram
ggplot(tp, aes(x=source)) + 
    geom_histogram(binwidth=2, colour="black", fill="white") +
    geom_vline(aes(xintercept=mean(source, na.rm=T)),
               color="green", linetype="dashed", size=1) +
    geom_vline(aes(xintercept=quantile(tp$source, c(.1), na.rm=T)[[1]]),   # Ignore NA values for mean
               color="blue", linetype="dashed", size=1)


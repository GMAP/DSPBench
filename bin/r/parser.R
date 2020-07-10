library(plyr)
library(ggplot2)


##
## MEMORY
##
memory.total.used <- read.csv("")

first_time <- memory.total.used$time[[1]]

newmem <- ddply(memory.total.used, .(time, value), function(x) {
    rel_time <- x$time - first_time
    mem_usage <- ((x$value/1024)/1024)
    data.frame(time=rel_time, value=mem_usage)
})

plot(newmem$time, newmem$value, type="n", xlab="Time (seconds)", ylab="Memory Usage (MBytes)")
lines(newmem$time, newmem$value)


##
## PROCESSING TIME
##
first_time <- split_sentence$time[[1]]

# calculate the relative time
# calculate the confidence interval with a 95% confidence level for a t distribution
new_split_sentence <- ddply(split_sentence, .(time, mean), function(x) {
     rel_time <- x$time - first_time
     error <- qt(0.975, df=x$count-1) * (x$stddev/sqrt(x$count))
     data.frame(time=rel_time, mean=x$mean, lower=(x$mean-error), upper=(x$mean+error), error=error)
})

ggplot(new_split_sentence, aes(time)) +
geom_line(aes(y=mean), colour="blue") +
geom_ribbon(aes(ymin=lower, ymax=upper), alpha=0.2)

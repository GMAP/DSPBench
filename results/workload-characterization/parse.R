library(plyr)
library(ggplot2)


mem_usage_transform <- function(input) {
    memory_usage <- read.csv(input, header=TRUE)
    first_time <- memory_usage$t[[1]]
    
    newmem <- ddply(memory_usage, .(t, value), function(x) {
        rel_time <- x$t - first_time
        mem_usage <- ((x$value/1024)/1024)
        data.frame(t=rel_time, value=mem_usage)
    })
    
    colnames(newmem) <- c('time', 'value')
    
    return(newmem)
}

read_tuple_size <- function(input) {
    data <- read.csv(input, header=TRUE)
    data$tuple_size[!is.na(data$tuple_size)]
    
    return(data)
}

##
## MEMORY USAGE
##
memory_usage_plot <- function(input, output) {
    memory_usage <- read.csv(input, header=TRUE)
    first_time <- memory_usage$t[[1]]
    
    newmem <- ddply(memory_usage, .(t, value), function(x) {
        rel_time <- x$t - first_time
        mem_usage <- ((x$value/1024)/1024)
        data.frame(time=rel_time, value=mem_usage)
    })

    ggplot(newmem, aes(time)) +
        geom_line(aes(y=value), colour="red") +
        ylab("Memory Usage (MBytes)") +
        xlab("Time (seconds)")

    ggsave(file=output)
}


##
## PROCESS TIME
##
process_time_plot <- function(input, output) {
    data <- read.csv(input, header=TRUE)
    first_time <- data$timestamp[[1]]
    
    new_data <- ddply(data, .(timestamp, process.mean), function(x) {
        rel_time <- x$timestamp - first_time
        error <- qt(0.975, df=x$process.count-1) * (x$process.stddev/sqrt(x$process.count))
        if (!is.na(x$process.mean)) {
            data.frame(timestamp=rel_time, process.mean=x$process.mean, 
                process.p95=x$process.p95, process.p99=x$process.p99, 
                process.mean.lower=(x$process.mean-error), process.mean.upper=(x$process.mean+error),
                process.error=error)
        }
    })
    
    ggplot(new_data, aes(timestamp)) +
        geom_line(aes(y=process.p99), colour="green") +
        geom_line(aes(y=process.p99), colour="red") +
        geom_line(aes(y=process.mean), colour="blue") +
        geom_ribbon(aes(ymin=process.mean.lower, ymax=process.mean.upper), alpha=0.2) +
        ylab("Process Time (ms)") +
        xlab("Time (seconds)")

    ggsave(file=output)
}

##
## PROCESS TIME STACKED
##
process_time_stacked <- function(input, output) {
    data <- read.csv(input, header=TRUE)
    first_time <- min(data$timestamp)
    
    new_data <- ddply(data, .(timestamp, process_time), function(x) {
        rel_time <- x$timestamp - first_time
        
        if (is.na(pmatch("sink", x$operator))) {
            data.frame(timestamp=rel_time, operator=x$operator, process_time=x$process_time)
        }
    })
    
    new_data <- new_data[with(new_data, order(operator, timestamp)), ]
    
    ggplot(new_data, aes(x=timestamp, y=process_time, fill=operator)) +
        geom_area(position="stack") +
        #ylim(0, 0.6) +
        ylim(0, max(new_data$process_time)) +
        ylab("Process Time (ms)") +
        xlab("Time (seconds)")
}

##
## THROUGHPUT
##
throughput_plot <- function(input, output) {
    data <- read.csv(input, header=TRUE)
    first_time <- data$timestamp[[1]]
    
    new_data <- ddply(data, .(timestamp, throughput), function(x) {
        rel_time <- x$timestamp - first_time
        if (!is.na(x$throughput)) {
            data.frame(timestamp=rel_time, throughput=x$throughput)
        }
    })
    
    ggplot(new_data, aes(timestamp)) +
        geom_line(aes(y=throughput)) +
        ylab("Throughput (tuples/sec)") +
        xlab("Time (seconds)")

    ggsave(file=output)
}

##
## SELECTIVITY
##
selectivity_plot <- function(input, output) {
    data <- read.csv(input, header=TRUE)
    
    dotchart(sel$ratio, labels=sel$operator, groups=sel$application, xlab="Ratio", xlim=c(0.0,10.5))
}

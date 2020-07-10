library(plyr)
library(ggplot2)
library(reshape2)

#-Throughput -------------------------------------------------------------------
throughput.plot <- function(data, dir) {
    data.m  <- melt(data, id.vars='timestamp')
    columns <- colnames(data)

    # timeline comparison
    pdf(paste(dir, 'throughput_timeline.pdf', sep='/'))
    plot <- ggplot(data.m, aes(x=timestamp, y=value, color=variable)) +
        geom_line() +
        ylab("Throughput (tuples/sec)") +
        xlab("Timestamp")
    print(plot)
    dev.off()

    # histogram for each operator
    for (i in 2:length(columns)) {
        opname = columns[[i]]

        data.sub <- ddply(data, .(timestamp), function(x) {
            data.frame(value=x[,opname])
        })

        # histogram for operator with line for mean and 10% percentile
        pdf(paste(dir, paste('throughput', opname, 'histogram.pdf', sep='_'), sep='/'))
        plot <- ggplot(data.sub, aes(x=value)) +
            geom_histogram(binwidth=2, colour="black", fill="white") +
            geom_vline(aes(xintercept=mean(value, na.rm=T)),
                       color="green", linetype="dashed", size=1) +
            geom_vline(aes(xintercept=quantile(data.sub$value, c(.1), na.rm=T)[[1]]),   # Ignore NA values for mean
                       color="blue", linetype="dashed", size=1)
        print(plot)
        dev.off()
    }
}

throughput.summary <- function(experiment, data) {
    columns <- colnames(data)
    summaries <- data.frame(experiment = character(), operator = character(),
                            mean = numeric(), stddev = numeric(), min = numeric(),
                            max = numeric(), count = numeric(), median = numeric(),
                            p95 = numeric(), p99 = numeric(), p999 = numeric(),
                            p05 = numeric(), p10 = numeric(), standard_error = numeric(),
                            stringsAsFactors=FALSE)

    for (i in 2:length(columns)) {
        operator = columns[[i]]
        summary <- full_summary(data[,operator])
        summaries[nrow(summaries) + 1, ] <- c(experiment, operator, summary)
    }

    return(summaries)
}

#-Latency ----------------------------------------------------------------------
latency.plot <- function(data, name, dir) {
    data.sub <- latency[, c("timestamp", "p99_max", "p999_max", "mean")]
    data.dd  <- melt(data.sub, id=c("timestamp"))

    pdf(paste(dir, paste(name, 'latency_timeline.pdf', sep='_'), sep='/'))
    plot <- ggplot(data.dd) +
        geom_line(aes(x=timestamp, y=value, colour=variable)) +
        ylab("Latency (milliseconds)") +
        xlab("Timestamp") +
        scale_colour_manual(values=c("red","green","blue"))
    print(plot)
    dev.off()
    #ggsave(file=paste(dir, '/latency_timeline.pdf', sep=''))

    # histogram
    pdf(paste(dir, paste(name, 'latency_histogram.pdf', sep=''), sep='/'))
    hist(data$p99_max, breaks=100)
    dev.off()
}

#' Create summary for latency data.
#'
#' @param experiment The name of the experiment.
#' @param data A data frame containing the latency data.
#' @return A data frame with the summary of the latency data.
latency.summary <- function(experiment, sink.name, data) {
    columns <- c('latency')

    summaries <- data.frame(experiment = character(), sink_name = character(), column = character(),
                            mean = numeric(), stddev = numeric(), min = numeric(),
                            max = numeric(), count = numeric(), median = numeric(),
                            p95 = numeric(), p99 = numeric(), p999 = numeric(),
                            p05 = numeric(), p10 = numeric(), standard_error = numeric(),
                            stringsAsFactors=FALSE)

    for (i in 1:length(columns)) {
        summary <- full_summary(data[,columns[i]])
        summaries[nrow(summaries) + 1, ] <- c(experiment, sink.name, columns[i], summary)
    }

    return(summaries)
}

#-Kafka ------------------------------------------------------------------------
# mem_free     Amount of free memory in KBytes
# mem_used     Amount of used memory in KBytes
# net_recv     Network download rate in KBytes/s
# net_sent     Network upload rate in KBytes/s
# hdd_read     HDD read rate in MBytes/s
# hdd_write    HDD write rate in MBytes/s
#-------------------------------------------------------------------------------
kafka.parse <- function(data) {
    #cols <- c("timestamp", "net_sent", "net_recv", "sdd_hdd_read", "sde_hdd_read",
    #          "sda_hdd_read", "sdb_hdd_read", "sdc_hdd_read", "sdd_hdd_write",
    #          "sde_hdd_write", "sda_hdd_write", "sdb_hdd_write", "sdc_hdd_write")
    cols.raw <- colnames(data)
    cols <- c(c("timestamp", "net_sent", "net_recv"), cols.raw[grep("_hdd_", cols.raw)])

    data.sub <- data[, cols]

    data.parsed <- ddply(data.sub, cols, function(x) {
        net_sent <- x$net_sent / 1024
        net_recv <- x$net_recv / 1024
        data.frame(net_sent=net_sent, net_recv=net_recv)
    })
}

kafka.plot <- function(data.parsed, dir) {
    data.dd <- melt(data.parsed, id=c("timestamp"))

    ggplot(data.dd) +
        geom_line(aes(x=timestamp, y=value, colour=variable)) +
        ylab("MBytes") +
        xlab("Timestamp") +
        scale_fill_brewer()

    ggsave(file=paste(dir, '/kafka_net_hdd.pdf', sep=''))
}

kafka.summary <- function(experiment, data) {
    columns <- colnames(data)
    summaries <- data.frame(experiment = character(), column = character(),
                            mean = numeric(), stddev = numeric(), min = numeric(),
                            max = numeric(), count = numeric(), median = numeric(),
                            p95 = numeric(), p99 = numeric(), p999 = numeric(),
                            p05 = numeric(), p10 = numeric(), standard_error = numeric(),
                            stringsAsFactors=FALSE)

    for (i in 3:length(columns)) {
        summary <- full_summary(data[,columns[i]])
        summaries[nrow(summaries) + 1, ] <- c(experiment, columns[i], summary)
    }

    return(summaries)
}

#-Cluster ----------------------------------------------------------------------
# mem/cpu/net usage
#-------------------------------------------------------------------------------
cluster.parse <- function(data) {
    cols <- c("timestamp", "count", "net_sent", "net_recv", "mem_used", "cpu_used")
    data.sub <- data[, cols]

    count <- data.sub$count[[1]]

    data.parsed <- ddply(data.sub, cols, function(x) {
        net_sent <- x$net_sent / 1024
        net_recv <- x$net_recv / 1024
        mem_used <- x$mem_used / count
        cpu_used <- x$cpu_used / count
        data.frame(net_sent=net_sent, net_recv=net_recv, mem_used=mem_used, cpu_used=cpu_used)
    })
}

cluster.plot <- function(data.parsed, dir) {
    data_mem_cpu <- melt(data.parsed[, c("timestamp", "mem_used", "cpu_used")], id=c("timestamp"))
    data_net     <- melt(data.parsed[, c("timestamp", "net_sent", "net_recv")], id=c("timestamp"))

    ggplot(data_mem_cpu) +
        geom_line(aes(x=timestamp, y=value, colour=variable)) +
        ylab("Usage (%)") +
        xlab("Timestamp") +
        ylim(0, 100) +
        scale_fill_brewer()

    ggsave(file=paste(dir, '/cluster_cpu_mem.pdf', sep=''))

    ggplot(data_net) +
        geom_line(aes(x=timestamp, y=value, colour=variable)) +
        ylab("MBytes") +
        xlab("Timestamp") +
        scale_fill_brewer()

    ggsave(file=paste(dir, '/cluster_net.pdf', sep=''))
}

cluster.summary <- function(experiment, data) {
    columns <- colnames(data)
    summaries <- data.frame(experiment = character(), column = character(),
                            mean = numeric(), stddev = numeric(), min = numeric(),
                            max = numeric(), count = numeric(), median = numeric(),
                            p95 = numeric(), p99 = numeric(), p999 = numeric(),
                            p05 = numeric(), p10 = numeric(), standard_error = numeric(),
                            stringsAsFactors=FALSE)

    for (i in 3:length(columns)) {
        rows <- data[,columns[i]]

        if (columns[i] == 'cpu_used') {
          rows <- rows[rows <= 100]
        }

        summary <- full_summary(rows)
        summaries[nrow(summaries) + 1, ] <- c(experiment, columns[i], summary)
    }

    return(summaries)
}

standard_error <- function(x) sqrt(var(x,na.rm=TRUE)/length(na.omit(x)))

full_summary <- function(data) {
    data.clean <- data[!is.na(data)]
    p <- quantile(data.clean, c(.5,.95,.99,.999,.05,.10))

    c(mean(data.clean), sd(data.clean), min(data.clean), max(data.clean),
      length(data.clean), p[[1]], p[[2]], p[[3]], p[[4]], p[[5]], p[[6]], standard_error(data.clean))
}

exports.csv <- function(df, filename) {
    write.table(df, file = filename, append = TRUE, quote = FALSE, sep = ",",
                row.names = FALSE, col.names = !file.exists(filename))
}

list.dirs <- function(path=".", pattern=NULL, all.dirs=FALSE,
  full.names=FALSE, ignore.case=FALSE) {
  # use full.names=TRUE to pass to file.info
  all <- list.files(path, pattern, all.dirs,
           full.names=TRUE, recursive=FALSE, ignore.case)
  dirs <- all[file.info(all)$isdir]
  # determine whether to return full names or just dir names
  if(isTRUE(full.names))
    return(dirs)
  else
    return(basename(dirs))
}

list.files.only <- function(path=".", pattern=NULL, all.dirs=FALSE,
  full.names=FALSE, ignore.case=FALSE) {
  # use full.names=TRUE to pass to file.info
  all <- list.files(path, pattern, all.dirs,
           full.names=TRUE, recursive=FALSE, ignore.case)
  dirs <- all[!file.info(all)$isdir]
  # determine whether to return full names or just dir names
  if(isTRUE(full.names))
    return(dirs)
  else
    return(basename(dirs))
}

create.plots <- function(experiment, dir, summary.dir) {
    print(paste("[START] Reading data for", experiment))
    throughput.data <- read.csv(paste(dir, 'throughput.csv', sep='/'), header=TRUE, stringsAsFactors=FALSE)
    kafka.data      <- read.csv(paste(dir, '/resources.kafka.csv', sep='/'), header=TRUE, stringsAsFactors=FALSE)
    cluster.data    <- read.csv(paste(dir, '/resources.cluster.csv', sep='/'), header=TRUE, stringsAsFactors=FALSE)

    print("Parsing and saving latency of sink(s)")
    files <- list.files.only(dir)
    latency.files <- files[grep("latency", files)]

    for (i in 1:length(latency.files)) {
        sink.name    <- strsplit(strsplit(latency.files[i], "\\.")[[1]][1], "-")[[1]][2]
        latency.data <- read.csv(paste(dir, latency.files[i], sep='/'), header=TRUE, stringsAsFactors=FALSE)
        lt_summaries <- latency.summary(experiment, sink.name, latency.data)
        exports.csv(lt_summaries, paste(summary.dir, 'summary.latency.csv', sep='/'))
    }

    print("Parsing Cluster and Kafka data")
    kafka.parsed   <- kafka.parse(kafka.data)
    cluster.parsed <- cluster.parse(cluster.data)

    print("Creating summaries")
    tp_summaries <- throughput.summary(experiment, throughput.data)
    cl_summaries <- cluster.summary(experiment, cluster.parsed)
    kf_summaries <- kafka.summary(experiment, kafka.parsed)

    print("Saving summaries")
    exports.csv(tp_summaries, paste(summary.dir, 'summary.throughput.csv', sep='/'))
    exports.csv(cl_summaries, paste(summary.dir, 'summary.cluster.csv', sep='/'))
    exports.csv(kf_summaries, paste(summary.dir, 'summary.kafka.csv', sep='/'))

    #return(list(latency = lt_summaries, throughput = tp_summaries, cluster = cl_summaries, kafka = kf_summaries))
}

experiment.parse <- function(dir, summary.dir) {
    dirs      <- list.dirs(dir)
    dirs.size <- length(dirs)

    for (i in 1:dirs.size) {
        exp_name = dirs[i]
        create.plots(exp_name, paste(dir, exp_name, sep='/'), summary.dir)
        print(paste("[STATUS]", i, "of", dirs.size, "processed"))
    }

}

plot.net_usage <- function(dir, data, name) {
    pdf(paste(dir, paste(name, '.pdf', sep=''), sep='/'))

    plot <- ggplot(data, aes(x = experiment, y = mean, fill = column)) +
      #geom_boxplot() +
      geom_bar(position=position_dodge(), stat="identity") +
      geom_errorbar(aes(ymin = mean - standard_error, ymax = mean + standard_error),
                    width=.2, position=position_dodge(.9)) +
      theme(axis.text.x = element_text(angle = 45, hjust = 1, size = 5)) +
      ggtitle("Average Network Usage") +
      labs(x="Experiment", y="Usage (MB/s)")

    print(plot)
    dev.off()
}

plot.cpu_mem <- function(dir, data, name) {
    pdf(paste(dir, paste(name, '.pdf', sep=''), sep='/'))

    plot <- ggplot(data, aes(x = experiment, y = mean, fill = column)) +
      geom_bar(position=position_dodge(), stat="identity") +
      ylim(0, 100) +
      geom_errorbar(aes(ymin = mean - standard_error, ymax = mean + standard_error),
                    width=.2, position=position_dodge(.9)) +
      theme(axis.text.x = element_text(angle = 45, hjust = 1, size = 5)) +
      ggtitle("CPU and Memory Usage") +
      labs(x="Experiment", y="Usage (%)")

    print(plot)
    dev.off()
}

plot.latency <- function(dir, data, name) {
    pdf(paste(dir, paste(name, '.pdf', sep=''), sep='/'))

    plot <- ggplot(data, aes(x = experiment, y = mean, fill = sink_name)) +
      #geom_boxplot() +
      geom_bar(position=position_dodge(), stat="identity") +
      geom_errorbar(aes(ymin = mean - standard_error, ymax = mean + standard_error),
                    width=.2, position=position_dodge(.9)) +
      theme(axis.text.x = element_text(angle = 45, hjust = 1, size = 5)) +
      ggtitle("Latency 95th Percentile") +
      labs(x="Experiment", y="seconds")

    print(plot)
    dev.off()
}

plot.throughput <- function(dir, data, name) {
    pdf(paste(dir, paste(name, '.pdf', sep=''), sep='/'))

    plot <- ggplot(data, aes(x = experiment, y = mean, fill = operator)) +
      geom_bar(position=position_dodge(), stat="identity") +
      geom_errorbar(aes(ymin = mean - standard_error, ymax = mean + standard_error),
                    width=.2, position=position_dodge(.9)) +
      theme(axis.text.x = element_text(angle = 45, hjust = 1, size = 5)) +
      ggtitle("Throughput Average") +
      labs(x="Experiment", y="tuples / second")

    print(plot)
    dev.off()
}

summary.plot <- function(dir) {
    tp_summary <- read.csv(paste(dir, 'summary.throughput.csv', sep='/'), header=TRUE, stringsAsFactors=FALSE)
    lt_summary <- read.csv(paste(dir, 'summary.latency.csv', sep='/'), header=TRUE, stringsAsFactors=FALSE)
    cl_summary <- read.csv(paste(dir, 'summary.cluster.csv', sep='/'), header=TRUE, stringsAsFactors=FALSE)

    # filter only experiments also on spark run
    #tp_summary <- tp_summary[tp_summary$experiment %in% experiments.list,]
    #lt_summary <- lt_summary[lt_summary$experiment %in% experiments.list,]
    #cl_summary <- cl_summary[cl_summary$experiment %in% experiments.list,]

    cl_summary.cpu_mem <- cl_summary[cl_summary$column %in% c('cpu_used','mem_used'),]
    cl_summary.net     <- cl_summary[cl_summary$column %in% c('net_recv', 'net_sent'),]

    # get only 95th percentile
    #lt_summary <- lt_summary[lt_summary$column == 'p95_max',]

    # blacklisted experiments (too high latency)
    #lt_summary <- lt_summary[!lt_summary$experiment %in% c('n8_x2_x2_x1_x4_x2', 'n2_x4_x2_x1_x4_x2', 'n8_x4_x2_x1_x4_x2'),]

    # convert milliseconds to seconds
    lt_summary <- ddply(lt_summary, .(experiment, sink_name, mean, standard_error), function(x) {
        mean           <- (x$mean/1000)
        standard_error <- (x$standard_error/1000)
        data.frame(mean=mean, standard_error=standard_error)
    })

    # use this to filter out high latencies
    lt_summary <- lt_summary[lt_summary$mean < 20,]

    # get only the worst throughputs
    #tp_summary <- tp_summary[tp_summary$mean < 5000,]
    tp_summary <- tp_summary[tp_summary$operator %in% c('sink', 'speedCalculatorBolt', 'mapMatcherBolt'),]

    # plot throughput
    plot.throughput(dir, tp_summary, 'throughput')

    # plot latency
    plot.latency(dir, lt_summary, 'latency')

    # plot cpu/mem usage
    plot.cpu_mem(dir, cl_summary.cpu_mem, 'cpu_mem')

    # plot net usage
    plot.net_usage(dir, cl_summary.net, 'network')
}

# list of experiments for comparison with spark run
experiments.list.logprocessing <- c("n1_x1_x2_x1_x4_x2", "n1_x2_x2_x1_x4_x2", "n1_x4_x2_x1_x4_x2",
  "n1_x8_x2_x1_x4_x2", "n2_x1_x2_x1_x4_x2", "n2_x2_x2_x1_x4_x2", "n2_x4_x2_x1_x4_x2",
  "n2_x8_x2_x1_x4_x2", "n4_x1_x2_x1_x4_x2", "n4_x2_x2_x1_x4_x2", "n4_x4_x2_x1_x4_x2",
  "n4_x8_x2_x1_x4_x2", "n8_x1_x2_x1_x4_x2", "n8_x2_x2_x1_x4_x2", "n8_x4_x2_x1_x4_x2",
  "n8_x8_x2_x1_x4_x2")

experiments.list.trafficmonitoring <- c("n1_x4_x2_x2", "n4_x2_x2_x2", "n4_x8_x2_x2")
experiments.list.wordcount <- c("n1_x1_x5_x6_x3", "n1_x2_x5_x6_x3", "n1_x3_x5_x6_x3",
  "n2_x1_x5_x6_x3", "n2_x2_x5_x6_x3", "n2_x3_x5_x6_x3", "n4_x1_x5_x6_x3", "n4_x2_x5_x6_x3",
  "n4_x3_x5_x6_x3", "n8_x1_x5_x6_x3", "n8_x2_x5_x6_x3", "n8_x3_x5_x6_x3")

experiments.list <- experiments.list.wordcount


# TODO:
# enable script to be called from cmd
# create per experiment charts
# create application summary: runtime
# create comparison charts with summary data

# Latency is in milliseconds

# create file with summary data / DONE
# summary data:
#   - 99th, 95th percentile of latency (percentile of a percentile)
#     - put all statistics of latency
#   - 10th and 5th percentiles of throughput, plus the average and standard deviation
#     - put all statistics of throughput
#   - Average (stddev) of CPU, memory and network usage for the cluster and kafka brokers
#   - How much data was consumer (look at producer) and the runtime
#   - see other metrics...

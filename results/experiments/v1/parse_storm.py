#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import re
import csv
import sys
import math

from os import path
from time import strptime, strftime, mktime
from collections import OrderedDict

from datetime import datetime

#--Utilities -------------------------------------------------------------------
def parse_logs(logdir, log_regex, datetime_format, timeseries={}, min_level='WARN'):
    p = re.compile(log_regex)
    logfiles = get_files(logdir)

    for logfile in logfiles:
        with open(path.join(logdir, logfile)) as f:
            for line in f:
                m = p.match(line)
                if not m: continue

                if m.group('level') == min_level:
                    ts = int(mktime(strptime(m.group('date').split(',')[0], datetime_format)))
                    
                    if ts not in timeseries:
                        timeseries[ts] = []
                    
                    timeseries[ts].append({
                        'class': m.group('class'),
                        'level': m.group('level'),
                        'message': m.group('message')
                    })
                    
    return timeseries
    
def parse_kafka_logs(logdir, timeseries={}, min_level='WARN'):
    log_regex = '\[(?P<date>\d{4}[/.-]\d{2}[/.-]\d{2}\s+\d{2}:\d{2}:\d{2},\d{3})\]\s+(?P<level>[A-Z]+)\s+(?P<message>.*)\s+\((?P<class>\S+)\)'
    dt_format = '%Y-%m-%d %H:%M:%S'
    return parse_logs(logdir, log_regex, dt_format, timeseries, min_level)

def parse_storm_logs(logdir, timeseries={}, min_level='WARN'):
    log_regex = '(?P<date>\d{4}[/.-]\d{2}[/.-]\d{2}\s+\d{2}:\d{2}:\d{2})\s+(?P<class>\S+)\s+\[(?P<level>\S+)\]\s+(?P<message>.*)'
    dt_format = '%Y-%m-%d %H:%M:%S'
    return parse_logs(logdir, log_regex, dt_format, timeseries, min_level)          

def get_subdirs(a_dir):
    return [name for name in os.listdir(a_dir)
            if os.path.isdir(os.path.join(a_dir, name))]

def get_files(a_dir):
    return [name for name in os.listdir(a_dir)
            if not os.path.isdir(os.path.join(a_dir, name))]
            
def file_search(f, regex):
    for line in f:
        result = regex.search(line)
        if result:
            return result
            
def get_hostname(log_file):
    regex = re.compile('environment:host.name=(".*?"|\S+)')
    result = file_search(log_file, regex)
    return result.group(1)
  
  
#--Functions -------------------------------------------------------------------
def parse_node_metrics(node_dir, interval=5):
    """
    Parse the resource usage metrics of a node, given its log directory. Returns
    the aggregated metrics, ordered by the time of occurrence.
    """
    metrics = {}
    devices = []
    
    with open(path.join(node_dir, 'monitor-hdd.log')) as f:
        for line in f:
            t, dev, r, w = line.split()
            ts = int(mktime(strptime(t, "%Y-%m-%dT%H:%M:%S+0000")))
            
            if dev not in devices:
                devices.append(dev)
            
            if not metrics.get(ts):
                metrics[ts] = {}
            if not metrics[ts].get('hdd_read'):
                metrics[ts]['hdd_read'] = {}
            if not metrics[ts].get('hdd_write'):
                metrics[ts]['hdd_write'] = {}
                
            metrics[ts]['hdd_read'][dev]  = float(r)
            metrics[ts]['hdd_write'][dev] = float(w)
    
    timestamps = metrics.keys()
    
    with open(path.join(node_dir, 'monitor-cpu_mem_net.log')) as f:
        for i, line in enumerate(f):
            fields = line.split()

            # in case there's more readings here than above
            if i >= len(timestamps):
                print i, len(timestamps)
                timestamps.append(timestamps[-1] + interval)
                metrics[timestamps[i]] = {
                    'hdd_read' : {k:0.0 for k in devices},
                    'hdd_write': {k:0.0 for k in devices}
                }
                
            total_mem = float(fields[5]) + float(fields[6])
            mem_used = float('%.2f' % ((float(fields[6])*100.0) / total_mem))
            
            metrics[timestamps[i]].update({
                'cpu_free': float(fields[3]), 'cpu_used': float(fields[4]),
                'mem_free': 100.0 - mem_used, 'mem_used': mem_used,
                'mem_free_bytes': float(fields[5]), 'mem_used_bytes': float(fields[6]),
                'net_recv': float(fields[7]), 'net_sent': float(fields[8])
            })

    return OrderedDict(sorted(metrics.items()))

def get_total_tuples(tt_file):
    """Get the total number of tuples received or emitted per component"""
    components = {}
    for line in open(tt_file):
        ts, c, val = line.split(',')
        if ts == 'timestamp':
            continue
        components[c] = int(val)
        
    return sum(components.values())
    

def get_throughput(tp_file):
    """
    Read the log of emitted tuples per component in each time interval and then
    creates a timeseries of the total throughput between readings (usually 5 seconds).
    """
    timeseries = OrderedDict()
    last_counts = {}
    
    # merge tuples emitted
    for line in open(tp_file):
        ts, c, val = line.split(',')
        if ts == 'timestamp': continue
            
        ts = int(ts) / 1000
        last_counts[c] = int(val)
        
        if ts in timeseries:
            timeseries[ts].update({c: int(val)})
        else:
            timeseries[ts] = dict(last_counts)
            
    throughput = OrderedDict()
    last_count = 0
    last_ts = 1
    
    for ts, counts in timeseries.items():
        if len(timeseries) == 0:
            last_ts = (ts - 1)
        total_count = sum(counts.values())
        throughput[ts] = (total_count - last_count) / (ts - last_ts)
        
        last_count = total_count
        last_ts = ts
 
    for k, v in reversed(throughput.items()):
        if v <= 0:
            throughput[k] = None
        else:
            break
 
    return throughput
   

def parse_producer_metrics(exp_name):
    """
    Parse the producer logs, extracting number of messages read as well as the
    final size of the dataset read.
    """
    timeseries = OrderedDict()
    last_counts = {'read': {}, 'messages': {}}
    
    with open(path.join(exp_name, 'producer.read.csv')) as f:
        for line in f:
            ts, c, val = line.split(',')
            if ts == 'timestamp': continue
            
            ts = int(ts) / 1000
            
            last_counts['read'][c] = int(val)
        
            if ts in timeseries:
                timeseries[ts]['read'].update({c: int(val)})
            else:
                timeseries[ts] = dict(last_counts)
        
    with open(path.join(exp_name, 'producer.messages.csv')) as f:
        for line in f:
            ts, c, val = line.split(',')
            if ts == 'timestamp': continue
            
            ts = int(ts) / 1000
            
            if ts in timeseries:
                timeseries[ts]['messages'].update({c: int(val)})
            else:
                timeseries[ts] = dict(last_counts)
                
    metrics = OrderedDict()
    
    for ts, counts in timeseries.items():
        metrics[ts] = {
            'read'    : sum(counts['read'].values()),
            'messages': sum(counts['messages'].values())
        }
        
    return metrics
   
   
def parse_producer_metrics_old(exp_name):
    """
    Parse the producer logs, extracting number of messages read as well as the
    final size of the dataset read.
    """
    read = {}
    messages = {}
    
    for line in open(path.join(exp_name, 'producer.read.csv')):
        ts, c, val = line.split(',')
        if ts == 'timestamp':
            continue
        read[c] = int(val)
        
    for line in open(path.join(exp_name, 'producer.messages.csv')):
        ts, c, val = line.split(',')
        if ts == 'timestamp':
            continue
        messages[c] = int(val)
        
    return {'read': sum(read.values()), 'messages': sum(messages.values())}


def parse_tuple_latency(tl_file):
    """
    Read the log of latencies and extract the relevant values for each time window,
    returning an ordered dictionary of instant latencies per component.
    """
    latencies = OrderedDict()
    header = {}
    f_values = ['samples', 'min', 'max', 'mean', 'stddev', 'p50', 'p75', 'p95', 'p98', 
                'p99', 'p999']
    
    for line in open(tl_file):
        fields = line.split(',')
        
        if 'timestamp' in fields:
            for idx, field in enumerate(fields):
                header[field.strip()] = idx
            continue
            
        if len(fields) != len(header):
            print('Fields lenght mismatch')
            continue
                
        ts = int(fields[header['timestamp']]) / 1000
        
        if ts not in latencies:
            latencies[ts] = {}
            
        values = {}

        for f_val in f_values:
            values[f_val] = int(fields[header[f_val]].strip())
            
        latencies[ts].update({fields[header['id']]: values})
        
    
    return latencies
   

def get_latencies_summary(latencies):
    """
    Given a raw timeseries of latencies, create a summary of each point in time,
    with the average standard deviation and mean, the minimum and maximum, and
    the minimum/maximum range for the percentiles (as we can't average them).
    """
    summaries = OrderedDict()
    
    for ts, lat in latencies.items():
        sum_samples = float(sum([c['samples'] for c in lat.values()]))

        avg_var  = 0.0
        avg_mean = 0.0
        mins = []
        maxs = []
        pcs = {'p50': [], 'p75': [], 'p95': [], 'p98': [], 'p99': [], 'p999': []}
        
        for c in lat.values():
            avg_var  += (float(c['samples']) / sum_samples) * math.pow(c['stddev'], 2)
            avg_mean += (float(c['samples']) / sum_samples) * c['mean']
            mins.append(c['min'])
            maxs.append(c['max'])
            
            for pc in pcs.keys():
                pcs[pc].append(c[pc])

        summary = {'stddev': math.sqrt(avg_var), 'mean': avg_mean,
                   'min': min(mins), 'max': max(maxs), 'samples': sum_samples}
        
        for pc, vals in pcs.items():
            summary[pc] = {'min': min(vals), 'max': max(vals)}
        
        summaries[ts] = summary
    
    return summaries
    
    
def get_nodes(exp_name):
    nodes = {}
    for d in get_subdirs(exp_name):
        node = {'id': d, 'role': None}
        
        subdirs = get_subdirs(path.join(exp_name, d))
        
        if 'storm' in subdirs:
            if 'nimbus.log' in get_files(path.join(exp_name, d, 'storm')):
                node['role'] = 'master'
                node['name'] = get_hostname(open(path.join(exp_name, d, 'storm', 'nimbus.log')))
            else:
                node['role'] = 'slave'
                node['name'] = get_hostname(open(path.join(exp_name, d, 'storm', 'supervisor.log')))
        elif 'kafka' in subdirs:
            node['role'] = 'producer'
            node['name'] = get_hostname(open(path.join(exp_name, d, 'kafka', 'server.log')))
            
        if node['role']:
            nodes[d] = node
    
    return nodes
    
    
def get_component_metrics(exp_name):
    components = {}
    
    # get the name of the components
    for fname in get_files(exp_name):
        fields = fname.split('.')
        
        if len(fields) == 2 or fields[0] in components:
            continue

        if fields[0] == 'producer':
            components['producer'] = {'type': 'producer'}
        else:
            components[fields[0]] = {'type': 'operator'}
        
    # get metrics from each component
    for name, comp in components.items():
        if comp['type'] == 'operator':
            received   = get_total_tuples(path.join(exp_name, '%s.tuples-received.csv' % name))
            emitted    = get_total_tuples(path.join(exp_name, '%s.tuples-emitted.csv' % name))
            throughput = get_throughput(path.join(exp_name, '%s.tuples-emitted.csv' % name))
            
            components[name].update({
                'received': received, 'emitted': emitted, 'throughput': throughput})
            
        if comp['type'] == 'producer':
            metrics = parse_producer_metrics(exp_name)
            components[name].update({'metrics': metrics})
            
        if name == 'sink':
            latencies = parse_tuple_latency(path.join(exp_name, 'sink.tuple-latency.csv'))
            components['sink']['latencies'] = get_latencies_summary(latencies)
            
    return components
    
    
def aggregate_throughput(components):
    max_size = 0
    dates = None
    
    # find the longest set of metrics
    for comp in components.values():
        if comp['type'] != 'operator': continue

        if len(comp['throughput']) > max_size:
            max_size = len(comp['throughput'])
            dates = comp['throughput'].keys()
      
    records = []
    header  = ['timestamp']
    
    for date in dates:
        records.append([date])
            
    # add the values of each operator to the records
    for cname, comp in components.items():
        if comp['type'] != 'operator' or cname == 'sink':
            continue

        header.append(cname)
        values = comp['throughput'].values()
        
        # fill the missing values at the end
        if len(values) < max_size:
            values = values + [None] * (max_size - len(values))
                
        # add the values to the last column
        for i in range(0, max_size):
            records[i].append(values[i])


    return {'header': header, 'records': records}
    
    
def aggregate_latency(latencies):
    pcs = ['p50', 'p75', 'p95', 'p98', 'p99', 'p999']
    header = ['timestamp']
    records = []
    
    for ts in latencies.keys():
        records.append([ts])
        
    for k in latencies.values()[0].keys():
        if k in pcs:
            for t in ['min','max']:
                header.append(k + '_' + t)
        else:
            header.append(k)
        
    for idx, lat in enumerate(latencies.values()):
        for k, v in lat.items():
            if k in pcs:
                for t in ['min','max']:
                    records[idx].append(v[t])
            else:
                records[idx].append(v)
                
    return {'header': header, 'records': records}
           
           
def aggregate_producer_metrics(metrics):
    header = ['timestamp'] + [k for k in metrics.values()[0].keys()]
    records = []
    
    for ts in metrics.keys():
        records.append([ts])
            
    for idx, metric in enumerate(metrics.values()):
        for v in metric.values():
            records[idx].append(v)
            
    return {'header': header, 'records': records}
            
def aggregate_node_metrics(metrics):
    header = ['timestamp']
    records = []
    
    for ts in metrics.keys():
        records.append([ts])
        
    first_record = metrics.values()[0]
        
    for k in first_record.keys():
        if k in ['hdd_read', 'hdd_write']:
            for dev in first_record[k].keys():
                header.append(dev + '_' + k)
        else:
            header.append(k)
            
    for idx, metric in enumerate(metrics.values()):
        for k, v in metric.items():
            if k in ['hdd_read', 'hdd_write']:
                for dev, val in v.items():
                    records[idx].append(val)
            else:
                records[idx].append(v)
            
    return {'header': header, 'records': records}
       
def aggregate_node_logs(logs):
    header = ['timestamp'] + [k for k in logs.values()[0][0]]
    records = []
        
    for ts, loglist in logs.items():
        for log in loglist:
            records.append([ts] + log.values())
    
    return {'header': header, 'records': records}
    
    
def export_csv(outdir, name, data):
    with open(path.join(outdir, name + '.csv'), 'wb') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(data['header'])
        writer.writerows(data['records'])
    
#-------------------------------------------------------------------------------
    
    
num_nodes = 2
num_exp   = 0
conf_name = 'x1'

exp_name = 'n%d_r%d_%s' % (num_nodes, num_exp, conf_name)

# get the nodes
print('Get list of nodes...')
nodes = get_nodes(exp_name)

print('Get component metrics...')
components = get_component_metrics(exp_name)
    
# get metrics/logs from each node
for node_dir, node in nodes.items():
    print('Get metrics for node %s' % node['name'])
    # get node resource usage
    node['metrics'] = parse_node_metrics(path.join(exp_name, node_dir))

    # get all errors
    # then sort them by key (timestamp)
    timeseries = {}
    
    if node['role'] in ['master', 'slave']:
        parse_storm_logs(path.join(exp_name, node_dir, 'storm'), timeseries)
    elif node['role'] == 'producer':
        parse_kafka_logs(path.join(exp_name, node_dir, 'kafka'), timeseries)

    node['logs'] = OrderedDict(sorted(timeseries.items()))


# aggregate data
throughput = aggregate_throughput(components)
latency    = aggregate_latency(components['sink']['latencies'])
producer   = aggregate_producer_metrics(components['producer']['metrics'])

print('Saving data...')

# export data
outdir = 'output'
export_csv(outdir, 'throughput', throughput)
export_csv(outdir, 'latency', latency)
export_csv(outdir, 'producer', producer)

for k, node in nodes.items():
    data = aggregate_node_metrics(node['metrics'])
    export_csv(outdir, 'resources.' + node['name'], data)
    
    data = aggregate_node_logs(node['logs'])
    export_csv(outdir, 'errors.' + node['name'], data)



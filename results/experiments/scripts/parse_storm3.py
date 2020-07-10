#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import re
import csv
import sys
import math

from os import path
from time import strptime, strftime, mktime
from collections import OrderedDict, Counter

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

def get_hostname(log_file, default_name='', search_more=True):
    regex = re.compile('environment:host.name=(".*?"|\S+)')
    result = file_search(log_file, regex)

    if result is None and search_more:
        file_path = log_file.name + '.1'
        if os.path.exists(file_path):
            return get_hostname(open(file_path), default_name, False)

    if result is None:
        return default_name

    return result.group(1)

def is_sink(name):
    return name.lower().find("sink") != -1

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
                #print i, len(timestamps)
                timestamps.append(timestamps[-1] + interval)
                metrics[timestamps[i]] = {
                    'hdd_read' : {k:0.0 for k in devices},
                    'hdd_write': {k:0.0 for k in devices}
                }

            total_mem = float(fields[5]) + float(fields[6])
            mem_used  = float('%.2f' % ((float(fields[6])*100.0) / total_mem))

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

    with open(path.join(exp_name, 'producer.read.csv')) as f:
        for line in f:
            ts, c, val = line.split(',')
            if ts == 'timestamp': continue

            ts = int(ts) / 1000

            if ts in timeseries:
                timeseries[ts]['read'] += int(val)
            else:
                timeseries[ts] = {'read': int(val), 'messages': 0}

    with open(path.join(exp_name, 'producer.messages.csv')) as f:
        for line in f:
            ts, c, val = line.split(',')
            if ts == 'timestamp': continue

            ts = int(ts) / 1000

            if ts in timeseries:
                timeseries[ts]['messages'] += int(val)
            else:
                timeseries[ts] = {'read': 0, 'messages': int(val)}

    return timeseries


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

        try:
            for f_val in f_values:
                values[f_val] = int(fields[header[f_val]].strip())
        except KeyError:
            print("Error getting key for %s file" % tl_file)
            raise

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

        # if sum of samples is zero, ignore (means end of metrics)
        if sum_samples == 0:
            continue

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
                node['name'] = get_hostname(open(path.join(exp_name, d, 'storm', 'nimbus.log')), d)
            else:
                node['role'] = 'slave'
                node['name'] = get_hostname(open(path.join(exp_name, d, 'storm', 'supervisor.log')), d)
        elif 'kafka' in subdirs:
            node['role'] = 'producer'
            node['name'] = get_hostname(open(path.join(exp_name, d, 'kafka', 'server.log')), d)

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

        if is_sink(name):
            # the sink only receives, so the final throughput is here
            throughput = get_throughput(path.join(exp_name, '%s.tuples-received.csv' % name))
            latencies  = parse_tuple_latency(path.join(exp_name, '%s.tuple-latency.csv' % name))
            components[name].update({
                'latencies': get_latencies_summary(latencies), 'throughput': throughput})

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
        #if comp['type'] != 'operator' or is_sink(cname):
        if comp['type'] != 'operator':
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

    if len(latencies.values()) > 0:
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
    # aggregate metrics
    for k,v in metrics.items():
        if type(v) is dict:
            m = v
        else:
            m = dict(read=0, messages=0)
            for item in v:
                try:
                    if type(item) is list:
                        for item2 in item:
                            if type(item2) is list:
                                for item3 in item2:
                                    if type(item3) is list:
                                        for item4 in item3:
                                            m['read']     += item4['read']
                                            m['messages'] += item4['messages']
                                    else:
                                        m['read']     += item3['read']
                                        m['messages'] += item3['messages']
                    else:
                        m['read']     += item['read']
                        m['messages'] += item['messages']
                except (TypeError):
                    print("==========")
                    print(m)
                    print(v)
                    print("==========")
                    print(item)
                    print("==========")
                    raise
        metrics[k] = m

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

def aggregate_merged_metrics(metrics):
    header = ['timestamp']
    records = []

    for ts in range(len(metrics)):
        records.append([ts])

    first_record = metrics[0]

    for k in first_record.keys():
        if k in ['hdd_read', 'hdd_write']:
            for dev in first_record[k].keys():
                header.append(dev + '_' + k)
        else:
            header.append(k)

    for idx, metric in enumerate(metrics):
        for k in header:
            if k == 'timestamp':
                continue
            elif k.find('hdd_read') != -1 and 'hdd_read' in metric:
                dev = k.replace('_hdd_read', '')
                records[idx].append(metric['hdd_read'][dev])
            elif k.find('hdd_write') != -1 and 'hdd_write' in metric:
                dev = k.replace('_hdd_write', '')
                records[idx].append(metric['hdd_write'][dev])
            elif k in metric:
                records[idx].append(metric[k])
            else:
                records[idx].append(0)

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

def merge_dict(target, source, level=0):
    for k,v in source.items():
        if type(v) is dict:
            if k in target:
                target[k] = Counter(target[k]) + Counter(v)
            else:
                target[k] = v
        else:
            if k in target:
                target[k] = target[k] + v
            else:
                target[k] = v

def merge_ordered_dict(d1, d2):
    if d2 is None: return d1
    if d1 is None: return d2

    d3 = OrderedDict()
    for k,e in d1.items()+d2.items():
        d3.setdefault(k,[]).append(e)

    return d3

def merge_metrics(target, source):
    for k,v in enumerate(source.values()):
        if k < len(target):
            merge_dict(target[k], v)

            if 'count' in target[k]:
                target[k]['count'] += 1
            else:
                target[k]['count'] = 2


def get_experiment_metrics(exp_dir, parse_logs = False):
    print('Get list of nodes...')
    nodes = get_nodes(exp_dir)

    print('Get component metrics...')
    components = get_component_metrics(exp_dir)

    # get metrics/logs from each node
    for node_dir, node in nodes.items():
        print('Get metrics for node %s' % node['name'])
        # get node resource usage
        node['metrics'] = parse_node_metrics(path.join(exp_dir, node_dir))

        if parse_logs:
            print('Get logs for node %s' % node['name'])

            # get all errors
            # then sort them by key (timestamp)
            timeseries = {}

            if node['role'] in ['master', 'slave']:
                parse_storm_logs(path.join(exp_dir, node_dir, 'storm'), timeseries)
            elif node['role'] == 'producer':
                parse_kafka_logs(path.join(exp_dir, node_dir, 'kafka'), timeseries)

            node['logs'] = OrderedDict(sorted(timeseries.items()))

    return [nodes, components]

def get_experiments(app_dir):
    dirs = [ name for name in os.listdir(app_dir) if os.path.isdir(os.path.join(app_dir, name)) ]

    experiments = {}

    for d in dirs:
        nodes, count, config = d.split("_", 2)

        if nodes not in experiments:
            experiments[nodes] = {}

        if config not in experiments[nodes]:
            experiments[nodes][config] = []

        experiments[nodes][config].append(count)

    return experiments

def merge_resource_metrics(nodes):
    cluster_resources = None
    kafka_resources   = None

    for k, node in nodes.items():
        name = node['name']

        if name.find('-data-') != -1:
            if kafka_resources is None:
                kafka_resources = node['metrics'].values()
            else:
                merge_metrics(kafka_resources, node['metrics'])
        else:
            if cluster_resources is None:
                cluster_resources = node['metrics'].values()
            else:
                merge_metrics(cluster_resources, node['metrics'])

    return {
        'kafka'  : aggregate_merged_metrics(kafka_resources),
        'cluster': aggregate_merged_metrics(cluster_resources)
    }

def export_experiment_executions(executions, outdir):
    throughput     = None
    latency        = {}
    resources      = None
    node_resources = {}
    producer       = None

    for execution in executions:
        # throughput
        tp = aggregate_throughput(execution['components'])

        if throughput is None:
            throughput = tp
        else:
            throughput['records'] += tp['records']

        # latency
        for name, component in execution['components'].items():
            if is_sink(name):
                lt = aggregate_latency(component['latencies'])

                if name not in latency:
                    latency[name] = lt
                else:
                    latency[name]['records'] += lt['records']
        #elif name == 'producer':
        #print(execution['components']['producer']['metrics'])
        # Export producer data
        if producer is None:
            producer = execution['components']['producer']['metrics']
        else:
            producer = merge_ordered_dict(producer, execution['components']['producer']['metrics'])
                    #producer['kafka']['records'] += res['kafka']['records']
                #if 'producer' in components:
                    # producer = aggregate_producer_metrics(components['producer']['metrics'])
                    # export_csv(outdir, 'producer', producer)

        # resource usage
        res = merge_resource_metrics(execution['nodes'])
        if resources is None:
            resources = res
        else:
            resources['kafka']['records'] += res['kafka']['records']
            resources['cluster']['records'] += res['cluster']['records']

        # resource usage per node
        for k, node in execution['nodes'].items():
            metrics = aggregate_node_metrics(node['metrics'])
            if 'logs' in node:
                logs = aggregate_node_logs(node['logs'])

            if node['name'] not in node_resources:
                if 'logs' in node:
                    node_resources[node['name']] = dict(metrics=metrics, logs=logs)
                else:
                    node_resources[node['name']] = dict(metrics=metrics)
            else:
                node_resources[node['name']]['metrics']['records'] += metrics['records']
                if 'logs' in node:
                    node_resources[node['name']]['logs']['records']    += logs['records']

    producer_data = aggregate_producer_metrics(producer)
    export_csv(outdir, 'producer', producer_data)

    export_csv(outdir, 'throughput', throughput)

    for name, lt in latency.items():
        export_csv(outdir, 'latency-%s' % name, lt)

    export_csv(outdir, 'resources.kafka', resources['kafka'])
    export_csv(outdir, 'resources.cluster', resources['cluster'])

    for node, data in node_resources.items():
        export_csv(outdir, 'resources.' + node, data['metrics'])
        if 'logs' in data:
            export_csv(outdir, 'errors.' + node, data['logs'])
#-------------------------------------------------------------------------------
# TODO:

# - create app summary: number of executions, runtime, data consumed
#   - use the topologies.csv to do that

app_dir = sys.argv[1]
outdir  = 'output4'
experiments = get_experiments(app_dir)

node_num = 1

# iterate over each number of nodes
for nodes, configs in experiments.items():
    cfg_num = 1
    # iterate over each configuration for a number of nodes
    for config, counts in configs.items():
        print("[START] Processing files for configuration '%s' for '%s' nodes and %d runs." % (config, nodes, len(counts)))

        # create sub directory to save data
        subdir = path.join(outdir, "_".join([nodes, config]))
        if not os.path.exists(subdir):
            os.makedirs(subdir)
        else:
            print("[STATUS] '%s' already exists, skipping it." % subdir)
            cfg_num += 1
            continue

        executions = []

        # iterate over each run of a no.nodes/configuration set
        # aggregates all the metrics for export
        for run in counts:
            print("[STATUS] %s of %d" % (run, len(counts)))
            exp_dir = "_".join([nodes, run, config])
            exp_nodes, exp_components = get_experiment_metrics(path.join(app_dir, exp_dir))
            executions.append(dict(nodes=exp_nodes, components=exp_components))

        # get a summary for all executions of mean runtime, data consumed
        export_experiment_executions(executions, subdir)

        print("[STATUS] nodes = %d of %d / configs = %d of %d" % (node_num, len(experiments), cfg_num, len(configs)))
        #sys.exit()

        cfg_num += 1

    node_num += 1

print('All done.')

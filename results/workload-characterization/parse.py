#!/usr/bin/python

import sys
import csv
import numpy

from os import listdir
from os.path import isfile, join

types   = ['tuples-emitted', 'tuples-received', 'rate', 'throughput', 'tuple-size',
           'process-time']
columns = ['tuples-emitted', 'tuples-received', 'process-mean', 'process-count',
           'process-stddev', 'process-p95', 'process-p99', 'throughput']


def process(application):
    print("Processing data for application {}".format(application))
    
    path = "{}/logs/".format(application)
    out_dir = "{}/summary/".format(application)
    
    summary = build_sumary(path)

    write_operator_files(out_dir, summary)
    write_process_time(out_dir, summary)
    write_tuple_size(out_dir, summary)
    
    rows = get_summary_array(application, summary)
    
    return rows


def build_sumary(path):
    summary = {}
    
    files = [ f for f in listdir(path) if isfile(join(path, f)) ]
    print("List of files to read: {}".format(files))

    for filename in files:
        ftype = ''.join([t for t in types if t in filename])
        name = filename.replace('-'+ftype+'.csv', '')
        name = filename.replace('.'+ftype+'.csv', '')
        
        if '-' in name:
            name = name.split("-")[0]
        
        print("Opening file {} with name {} and type {}".format(filename, name, ftype))
        
        if not ftype:
            continue
        
        if name not in summary:
            summary[name] = {}

        with open(join(path, filename), 'rb') as csvfile:
            reader = csv.reader(csvfile, delimiter=',')
            headers = reader.next()
            
            for row in reader:
                if row[0] not in summary[name]:
                    summary[name][row[0]] = {}
                
                if ftype == 'tuples-emitted' or ftype == 'tuples-received':
                    summary[name][row[0]].update({ftype: row[1]})
                elif ftype == 'rate':
                    summary[name][row[0]].update({
                        'process-mean': row[3],
                        'process-count': row[1],
                        'process-stddev': row[5],
                        'process-p95': row[8],
                        'process-p99': row[10]
                    })
                    
                elif ftype == 'process-time':
                    summary[name][row[0]].update({
                        'process-mean': row[3],
                        'process-count': row[1],
                        'process-stddev': row[5],
                        'process-p95': row[8],
                        'process-p99': row[10]
                    })
                elif ftype == 'tuple-size':
                    summary[name][row[0]].update({'tuple-size': row[12]})
                elif ftype == 'throughput':
                    if '{' in row[1]:
                        items = row[1].strip('{}').replace(' ', '').split(';')
                        
                        for item in items:
                            key, val = item.split('=')
                            
                            #if key is not 'total':
                            if key.isdigit():
                                if key not in summary[name]:
                                    summary[name][key] = {ftype: val}
                                else:
                                    summary[name][key].update({ftype: val})
                                    
                    else:
                        key = row[0]
                        val = row[1]
                        
                        if key not in summary[name]:
                            summary[name][key] = {ftype: val}
                        else:
                            summary[name][key].update({ftype: val})
                            
    return summary
               
def write_operator_files(out_dir, summary):
    for op in summary:
        records = sorted(summary[op].iteritems(), key=lambda key_value: key_value[0])
        
        with open(join(out_dir, op+'.csv'), 'wb') as csvfile:
            writer = csv.writer(csvfile, delimiter=',')
            writer.writerow(['timestamp'] + columns)
            
            for record in records:
                row = [record[0]]
                
                for col in columns:
                    if col in record[1]:
                        row.append(record[1][col])
                    else:
                        row.append('')
                
                writer.writerow(row)
            
def write_process_time(out_dir, summary):
    with open(join(out_dir, 'process_time.csv'), 'wb') as csvfile:
        writer = csv.writer(csvfile, delimiter=',')
        writer.writerow(['timestamp', 'operator', 'process_time'])
        
        for op in summary:
            records = sorted(summary[op].iteritems(), key=lambda key_value: key_value[0])
            
            for record in records:
                if 'process-p99' in record[1]:
                    writer.writerow([record[0], op, record[1]['process-p99']])
          
def write_tuple_size(out_dir, summary):
    with open(join(out_dir, 'tuple_size.csv'), 'wb') as csvfile:
        writer = csv.writer(csvfile, delimiter=',')
        writer.writerow(['operator', 'tuple_size'])
        
        for op in summary:
            records = sorted(summary[op].iteritems(), key=lambda key_value: key_value[0])
            
            record = records[-1][1]
            if 'tuple-size' in record:
                if len(record['tuple-size']) > 0:
                    values = record['tuple-size'].split('|')
                    
                    for value in values:
                        writer.writerow([op, value])
       
       
def get_summary_array(app_name, summary):
    rows = []
    
    # create summary
    op_id = 0

    for op in summary:
        records = sorted(summary[op].iteritems(), key=lambda key_value: key_value[0])
        
        tp = []

        # sum all throughput values
        for record in records:
            if 'throughput' in record[1]:
                tp.append(int(record[1]['throughput']))

        # remove zeroes
        tp = list(filter(lambda x: (x != 0), tp))

        # check if there is data
        if len(tp) > 0:
            # get last record because some metrics are cumulative
            last_record = find_last_valid_record(records) #records[-1][1]
            
            ratio = float(last_record['tuples-emitted'])/float(last_record['tuples-received'])
            tpavg = int(numpy.mean(tp, axis=0))
            tp5   = numpy.percentile(tp, 5, axis=0)
            tpsd  = numpy.std(tp, axis=0)
            pt99  = float(last_record['process-p99'])
            pt95  = float(last_record['process-p95'])
            ptavg = float(last_record['process-mean'])
            ptsd  = float(last_record['process-stddev'])
            
            rows.append([app_name, str(op_id), op, str(ratio), str(tpavg), str(tp5), str(tpsd), str(pt99), str(pt95), str(ptavg), str(ptsd)])
            op_id += 1
            
    return rows
                
def find_last_valid_record(records):
    for x in range(1, len(records)+1):
        last_record = records[-x][1]
        has_all = True
        
        for col in ['tuples-emitted', 'tuples-received', 'process-p99', 'process-p95', 'process-mean', 'process-stddev']:
            if col not in last_record:
                has_all = False
        
        if has_all is True:
            return last_record
            
                    
def write_summary(out_dir, rows):
    with open(join(out_dir, 'summary.csv'), 'wb') as csvfile:
        writer = csv.writer(csvfile, delimiter=',')
        writer.writerow(['app', 'operator_id', 'operator', 'ratio', 'throughput_avg', 'throughput_5th',
                         'throughput_stddev', 'process_time_99th', 'process_time_95th',
                         'process_time_avg', 'process_time_stddev'])
          
        for row in rows:      
            writer.writerow(row)
            
            

applications = ['ads-analytics', 'bargain-index', 'reinforcement-learner', 'smart-grid',
                'traffic-monitoring', 'voipstream']

summary_rows = []

for application in applications:
    summary_rows += process(application)


write_summary('', summary_rows)

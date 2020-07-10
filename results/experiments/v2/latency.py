#!/usr/bin/env python3

import sys
import pandas as pd
from plot_3d import plot_3d

base_path = '/home/mayconbordin/Downloads/plot_3d/data-stream-benchmark-master/summaries'

applications = ['storm_logprocessing', 'spark_logprocessing', 
                'storm_wordcount', 'spark_wordcount',
                'storm_trafficmonitoring', 'spark_trafficmonitoring']

final_df = None
final_size = 0

for application in applications:
    df = pd.read_csv(base_path + '/' + application + '/summary.latency.csv')
    df.insert(0, 'application', application)
    
    if final_df is None:
        final_df = df
    else:
        final_df = final_df.append(df, True)
    
    final_size += df.size

results = final_df.where(final_df['column'].isin(['mean','latency']))\
         .groupby(['experiment', 'application'])['mean'].min().to_frame('average')
         
results.reset_index(level=['experiment', 'application'], inplace=True)
results['node'] = results.apply(lambda row: row['experiment'].split("_")[0].replace("n", ""), axis=1)
results['average_seconds'] = results.apply(lambda row: row['average']/1000, axis=1)

#results.to_csv('latency-full.csv', sep=',', header=True, encoding='utf-8')
#sys.exit(0)

# get only the best result for each application and no. of nodes
results = results.groupby(['node', 'application'])['average_seconds'].min().to_frame('average')
results.reset_index(level=['node', 'application'], inplace=True)
print(results)

averages = []

for application in applications:
    partial = []
    for node in results['node'].unique():
        print("Loading info for application " + application + " with " + node + " nodes")
        result = results[(results['application'] == application) & (results['node'] == node)]
        
        if len(result) > 0:
            average = result.iloc[0]
            partial.append(round(average['average'], 4))
        else:
            partial.append(0)
            
        
        
    averages.append(partial)
    partial = []

print(averages)

plot_3d(results['node'].unique(), applications, averages,
        title='Average Latency x No. Nodes', xlabel='Nodes', ylabel='Applications', zlabel='Avg Latency (seconds)',
        filename='latency')
results.to_csv('latency.csv', sep=',', header=True, encoding='utf-8')













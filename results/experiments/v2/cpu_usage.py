#!/usr/bin/env python3

import sys
import pandas as pd
from plot_3d import plot_3d

base_path = '/home/mayconbordin/Downloads/plot_3d/data-stream-benchmark-master/summaries'

applications = ['storm_logprocessing', 'storm_wordcount', 'storm_trafficmonitoring', 'spark_wordcount', 'spark_logprocessing', 'spark_trafficmonitoring']

final_df = None
final_size = 0

for application in applications:
    df = pd.read_csv(base_path + '/' + application + '/summary.cluster.csv')
    df.insert(0, 'application', application)
    
    if final_df is None:
        final_df = df
    else:
        final_df = final_df.append(df, True)
    
    final_size += df.size
    

results = final_df.where(final_df['column'].isin(['cpu_used']) & (final_df['mean'] <= 100))\
         .groupby(['experiment', 'application'])['mean'].max().to_frame('average')
         
results.reset_index(level=['experiment', 'application'], inplace=True)
results['node'] = results.apply(lambda row: row['experiment'].split("_")[0].replace("n", ""), axis=1)

# get only the best result for each application and no. of nodes
results = results.groupby(['node', 'application'])['average'].max().to_frame('average')
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
            partial.append(int(average['average']))
        else:
            partial.append(0)
            
        
        
    averages.append(partial)
    partial = []

print(averages)

plot_3d(results['node'].unique(), applications, averages,
        title='Average CPU Usage x No. Nodes', xlabel='Nodes', ylabel='Applications', zlabel='CPU Usage (%)',
        filename='cpu_usage')
results.to_csv('results.csv', sep=',', header=True, encoding='utf-8')













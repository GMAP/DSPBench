#!/usr/bin/env python3

import sys
import pandas as pd
from plot_3d import plot_3d

base_path = '/home/mayconbordin/Downloads/plot_3d/data-stream-benchmark-master/summaries'

applications = ['spark_wordcount', 'storm_wordcount', 'spark_logprocessing', 'storm_logprocessing','spark_trafficmonitoring', 'storm_trafficmonitoring']

final_df = None
final_size = 0

for application in applications:
    df = pd.read_csv(base_path + '/' + application + '/summary.throughput.csv')
    df.insert(0, 'application', application)
    
    if final_df is None:
        final_df = df
    else:
        final_df = final_df.append(df, True)
    
    final_size += df.size
    

# here we select only the experiments that were performed on both applications
valid_experiments = final_df[final_df["operator"].str.contains("sink", na=False, case=False)]\
    .groupby(['experiment'])['application'].count().to_frame('count')
valid_experiments = valid_experiments[valid_experiments["count"] > 1].index.values

print(valid_experiments)

results = final_df[final_df["operator"].str.contains("sink", na=False, case=False)]\
         .where(final_df['experiment'].isin(valid_experiments))\
         .groupby(['experiment', 'application'])['mean'].sum().to_frame('average')
         
results.reset_index(level=['experiment', 'application'], inplace=True)
results['node'] = results.apply(lambda row: row['experiment'].split("_")[0].replace("n", ""), axis=1)

results.to_csv('throughput-full.csv', sep=',', header=True, encoding='utf-8')
sys.exit(0)

# get only the best result for each application and no. of nodes
results = results.groupby(['node', 'application'])['average'].max().to_frame('average')
results.reset_index(level=['node', 'application'], inplace=True)
print(results)

averages = []

for application in results['application'].unique():
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

plot_3d(results['node'].unique(), results['application'].unique(), averages,
        title='Average Throughput x No. Nodes', xlabel='Nodes', ylabel='Applications', zlabel='Avg Throughput (events/sec)',
        filename='throughput')
results.to_csv('throughput.csv', sep=',', header=True, encoding='utf-8')













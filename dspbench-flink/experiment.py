import os
import time
import datetime;

def change_prop(run, app):
    print("change prop")
    if app == "clickanalytics":
        #Remove last line
        #sed -i '$ d' .txt
        os.system("sed -i '$ d' /home/DSPBench/dspbench-flink/build/resources/main/config/clickanalytics.properties")
        #Add lines
        #echo "..." >> dir
        os.system('echo "metrics.output=/home/metrics/clickanalytics/'+str(run)+'/" >> /home/DSPBench/dspbench-flink/build/resources/main/config/clickanalytics.properties')
    elif app == "frauddetection":
        #Remove last line
        #sed -i '$ d' .txt
        os.system("sed -i '$ d' /home/DSPBench/dspbench-flink/build/resources/main/config/frauddetection.properties")
        #Add lines
        #echo "..." >> dir
        os.system('echo "metrics.output=/home/metrics/frauddetection/'+str(run)+'/" >> /home/DSPBench/dspbench-flink/build/resources/main/config/frauddetection.properties')
    elif app == "logprocessing":
        #Remove last line
        #sed -i '$ d' .txt
        os.system("sed -i '$ d' /home/DSPBench/dspbench-flink/build/resources/main/config/logprocessing.properties")
        #Add lines
        #echo "..." >> dir
        os.system('echo "metrics.output=/home/metrics/logprocessing/'+str(run)+'/" >> /home/DSPBench/dspbench-flink/build/resources/main/config/logprocessing.properties')
    elif app == "machineoutlier":
        #Remove last line
        #sed -i '$ d' .txt
        os.system("sed -i '$ d' /home/DSPBench/dspbench-flink/build/resources/main/config/machineoutlier.properties")
        #Add lines
        #echo "..." >> dir
        os.system('echo "metrics.output=/home/metrics/machineoutlier/'+str(run)+'/" >> /home/DSPBench/dspbench-flink/build/resources/main/config/machineoutlier.properties')
    elif app == "sentimentanalysis":
        #Remove last line
        #sed -i '$ d' .txt
        os.system("sed -i '$ d' /home/DSPBench/dspbench-flink/build/resources/main/config/sentimentanalysis.properties")
        #Add lines
        #echo "..." >> dir
        os.system('echo "metrics.output=/home/metrics/sentimentanalysis/'+str(run)+'/" >> /home/DSPBench/dspbench-flink/build/resources/main/config/sentimentanalysis.properties')
    elif app == "smartgrid":
        #Remove last line
        #sed -i '$ d' .txt
        os.system("sed -i '$ d' /home/DSPBench/dspbench-flink/build/resources/main/config/smartgrid.properties")
        #Add lines
        #echo "..." >> dir
        os.system('echo "metrics.output=/home/metrics/smartgrid/'+str(run)+'/" >> /home/DSPBench/dspbench-flink/build/resources/main/config/smartgrid.properties')
    elif app == "spikedetection":
        #Remove last line
        #sed -i '$ d' .txt
        os.system("sed -i '$ d' /home/DSPBench/dspbench-flink/build/resources/main/config/spikedetection.properties")
        #Add lines
        #echo "..." >> dir
        os.system('echo "metrics.output=/home/metrics/spikedetection/'+str(run)+'/" >> /home/DSPBench/dspbench-flink/build/resources/main/config/spikedetection.properties')
    elif app == "trafficmonitoring":
        #Remove last line
        #sed -i '$ d' .txt
        os.system("sed -i '$ d' /home/DSPBench/dspbench-flink/build/resources/main/config/trafficmonitoring.properties")
        #Add lines
        #echo "..." >> dir
        os.system('echo "metrics.output=/home/metrics/trafficmonitoring/'+str(run)+'/" >> /home/DSPBench/dspbench-flink/build/resources/main/config/trafficmonitoring.properties')
    elif app == "wordcount":
        #Remove last line
        #sed -i '$ d' .txt
        os.system("sed -i '$ d' /home/DSPBench/dspbench-flink/build/resources/main/config/wordcount.properties")
        #Add lines
        #echo "..." >> dir
        os.system('echo "metrics.output=/home/metrics/wordcount/'+str(run)+'/" >> /home/DSPBench/dspbench-flink/build/resources/main/config/wordcount.properties')

def start_job(app):
    print("start job")
    # ./bin/dspbench-flink-cluster.sh /home/DSPBench/dspbench-flink/build/libs/dspbench-flink-uber-1.0.jar wordcount /home/DSPBench/dspbench-flink/build/resources/main/config/wordcount.properties
    os.system('./bin/dspbench-flink-cluster.sh /home/DSPBench/dspbench-flink/build/libs/dspbench-flink-uber-1.0.jar ' + app + ' /home/DSPBench/dspbench-flink/build/resources/main/config/' + app + '.properties')

def cancel_job():
    print("cancel job")
    id = os.popen("~/flink-1.17.1/bin/flink list | grep RUNNING | awk -F ':' '{print $4}'").read()
    os.system('~/flink-1.17.1/bin/flink cancel ' + str(id))

def stop_cluster():
    os.system('~/flink-1.17.1/bin/stop-cluster.sh')

def start_cluster():
    os.system('~/flink-1.17.1/bin/start-cluster.sh')
    
def restart_cluster():
    print("restart cluster")
    stop_cluster() #os.system('~/flink-1.17.1/bin/stop-cluster.sh')
    time.sleep(15)
    os.system('sync; echo 1 > /proc/sys/vm/drop_caches')
    os.popen('ssh sup1 "sync; echo 1 > /proc/sys/vm/drop_caches"').read()
    os.popen('ssh sup2 "sync; echo 1 > /proc/sys/vm/drop_caches"').read()
    os.popen('ssh sup3 "sync; echo 1 > /proc/sys/vm/drop_caches"').read()
    time.sleep(5)
    start_cluster() #os.system('~/flink-1.17.1/bin/start-cluster.sh')
    time.sleep(15)

def time_txt(app, init_time, end_time):
    print("time.txt")
    with open(app+'.txt', 'a') as f:
        f.write(str(init_time) + " - " + str(end_time) + "\n")

start_cluster()

for i in range(1,6):
    '''
    #Click Analytics
    #Change Confs on .properties
    change_prop(i, "clickanalytics")
    #Reinicia Cluster
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("clickanalytics")
    time.sleep(600)
    cancel_job()
    end_time = datetime.datetime.now()
    time_txt("clickanalytics",init_time, end_time)

    #Fraud Detection
    #Change Confs on .properties
    change_prop(i, "frauddetection")
    #Reinicia Cluster
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("frauddetection")
    time.sleep(600)
    cancel_job()
    end_time = datetime.datetime.now()
    time_txt("frauddetection",init_time, end_time)
    
    #Log Processing
    #Change Confs on .properties
    change_prop(i, "logprocessing")
    #Reinicia Cluster
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("logprocessing")
    time.sleep(600)
    cancel_job()
    end_time = datetime.datetime.now()
    time_txt("logprocessing",init_time, end_time)

    #Machine Outlier
    #Change Confs on .properties
    change_prop(i, "machineoutlier")
    #Reinicia Cluster
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("machineoutlier")
    time.sleep(600)
    cancel_job()
    end_time = datetime.datetime.now()
    time_txt("machineoutlier",init_time, end_time)

    #Sentiment Analysis
    #Change Confs on .properties
    change_prop(i, "sentimentanalysis")
    #Reinicia Cluster
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("sentimentanalysis")
    time.sleep(600)
    cancel_job()
    end_time = datetime.datetime.now()
    time_txt("sentimentanalysis",init_time, end_time)

    #Smart Grid
    #Change Confs on .properties
    change_prop(i, "smartgrid")
    #Reinicia Cluster
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("smartgrid")
    time.sleep(600)
    cancel_job()
    end_time = datetime.datetime.now()
    time_txt("smartgrid",init_time, end_time)

    #Spike Detection
    #Change Confs on .properties
    change_prop(i, "spikedetection")
    #Reinicia Cluster
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("spikedetection")
    time.sleep(600)
    cancel_job()
    end_time = datetime.datetime.now()
    time_txt("spikedetection",init_time, end_time)

    #Traffic Monitoring
    #Change Confs on .properties
    change_prop(i, "trafficmonitoring")
    #Reinicia Cluster
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("trafficmonitoring")
    time.sleep(600)
    cancel_job()
    end_time = datetime.datetime.now()
    time_txt("trafficmonitoring",init_time, end_time)
    '''
    #Word Count
    #Change Confs on .properties
    change_prop(i, "wordcount")
    #Reinicia Cluster
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("wordcount")
    time.sleep(600)
    cancel_job()
    end_time = datetime.datetime.now()
    time_txt("wordcount",init_time, end_time)

stop_cluster()
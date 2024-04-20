import os
import time
import datetime;

exec = "batch"

def change_prop(run, app, stage1, stage2, stage3, stage4, stage5, stage6):
    print("change prop")
    if app == "wordcount":
        #Remove last line
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/wordcount.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/wordcount.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/wordcount.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/wordcount.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/wordcount.properties")
        #Add lines
        os.system('echo "wc.parser.threads='+str(stage1)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/wordcount.properties')
        os.system('echo "wc.splitter.threads='+str(stage2)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/wordcount.properties')
        os.system('echo "wc.counter.threads='+str(stage3)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/wordcount.properties')
        os.system('echo "wc.sink.threads='+str(stage4)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/wordcount.properties')
        os.system('echo "metrics.output=/home/gmap/metrics/'+exec+'/WC'+str(stage1)+str(stage2)+str(stage3)+str(stage4)+'/'+str(run)+'/" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/wordcount.properties')

    elif app == "spikedetection":
        #Remove last line
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/spikedetection.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/spikedetection.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/spikedetection.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/spikedetection.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/spikedetection.properties")
        #Add lines
        os.system('echo "sd.parser.threads='+str(stage1)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/spikedetection.properties')
        os.system('echo "sd.moving_average.threads='+str(stage2)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/spikedetection.properties')
        os.system('echo "sd.spike_detector.threads='+str(stage3)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/spikedetection.properties')
        os.system('echo "sd.sink.threads='+str(stage4)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/spikedetection.properties')
        os.system('echo "metrics.output=/home/gmap/metrics/'+exec+'/SD'+str(stage1)+str(stage2)+str(stage3)+str(stage4)+'/'+str(run)+'/" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/spikedetection.properties')

    elif app == "logprocessing":
        #Remove last line
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/logprocessing.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/logprocessing.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/logprocessing.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/logprocessing.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/logprocessing.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/logprocessing.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/logprocessing.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/logprocessing.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/logprocessing.properties")
        #Add lines
        os.system('echo "lp.parser.threads='+str(stage1)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/logprocessing.properties')
        os.system('echo "lp.volume_counter.threads='+str(stage2)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/logprocessing.properties')
        os.system('echo "lp.status_counter.threads='+str(stage3)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/logprocessing.properties')
        os.system('echo "lp.geo_finder.threads='+str(stage4)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/logprocessing.properties')
        os.system('echo "lp.geo_stats.threads='+str(stage5)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/logprocessing.properties')
        os.system('echo "lp.count.sink.threads='+str(stage6)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/logprocessing.properties')
        os.system('echo "lp.status.sink.threads='+str(stage6)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/logprocessing.properties')
        os.system('echo "lp.country.sink.threads='+str(stage6)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/logprocessing.properties')
        os.system('echo "metrics.output=/home/gmap/metrics/'+exec+'/LP'+str(stage1)+str(stage2)+str(stage3)+str(stage4)+str(stage5)+str(stage6)+'/'+str(run)+'/" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/logprocessing.properties')

    elif app == "clickanalytics":
        #Remove last line
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/clickanalytics.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/clickanalytics.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/clickanalytics.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/clickanalytics.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/clickanalytics.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/clickanalytics.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/clickanalytics.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/clickanalytics.properties")
        #Add lines
        os.system('echo "ca.parser.threads='+str(stage1)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/clickanalytics.properties')
        os.system('echo "ca.repeats.threads='+str(stage2)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/clickanalytics.properties')
        os.system('echo "ca.total_stats.threads='+str(stage3)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/clickanalytics.properties')
        os.system('echo "ca.geography.threads='+str(stage4)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/clickanalytics.properties')
        os.system('echo "ca.geo_stats.threads='+str(stage5)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/clickanalytics.properties')
        os.system('echo "ca.visit.sink.threads='+str(stage6)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/clickanalytics.properties')
        os.system('echo "ca.location.sink.threads='+str(stage6)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/clickanalytics.properties')
        os.system('echo "metrics.output=/home/gmap/metrics/'+exec+'/CA'+str(stage1)+str(stage2)+str(stage3)+str(stage4)+str(stage5)+str(stage6)+'/'+str(run)+'/" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/clickanalytics.properties')

    elif app == "sentimentanalysis":
        #Remove last line
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/sentimentanalysis.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/sentimentanalysis.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/sentimentanalysis.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/sentimentanalysis.properties")
        #Add lines
        os.system('echo "sa.parser.threads='+str(stage1)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/sentimentanalysis.properties')
        os.system('echo "sa.classifier.threads='+str(stage2)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/sentimentanalysis.properties')
        os.system('echo "sa.sink.threads='+str(stage3)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/sentimentanalysis.properties')
        os.system('echo "metrics.output=/home/gmap/metrics/'+exec+'/SA'+str(stage1)+str(stage2)+str(stage3)+'/'+str(run)+'/" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/sentimentanalysis.properties')

def start_job(app):
    print("start job")
    # ./bin/dspbench-flink-cluster.sh /home/DSPBench/dspbench-flink/build/libs/dspbench-flink-uber-1.0.jar wordcount /home/DSPBench/dspbench-flink/build/resources/main/config/wordcount.properties
    os.system('./bin/dspbench-flink-cluster.sh /home/gmap/DSPBench/dspbench-flink/build/libs/dspbench-flink-uber-1.0.jar ' + app + ' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/' + app + '.properties')

def stop_cluster():
    os.system('~/maven/flink-1.18.1/bin/stop-cluster.sh')

def start_cluster():
    os.system('~/maven/flink-1.18.1/bin/start-cluster.sh')

def restart_cluster():
    print("restart cluster")
    stop_cluster() #os.system('~/flink-1.17.1/bin/stop-cluster.sh')
    time.sleep(15)
    start_cluster() #os.system('~/flink-1.17.1/bin/start-cluster.sh')
    time.sleep(15)

def time_txt(app, conf, init_time, end_time):
    print("time.txt")
    with open('/home/gmap/DSPBench/dspbench-flink/txts/'+app+'-'+exec+'-'+str(conf)+'.txt', 'a') as f:
        f.write(str(init_time) + " - " + str(end_time) + "\n")

start_cluster()

for i in range(1,6):
    
    #Word Count
    #Change Confs on .properties
    change_prop(i, "wordcount", 2, 4, 4, 4, 0, 0)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("wordcount")
    end_time = datetime.datetime.now()
    time_txt("wordcount", 2444, init_time, end_time)

    #Word Count
    #Change Confs on .properties
    change_prop(i, "wordcount", 2, 5, 6, 3, 0, 0)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("wordcount")
    end_time = datetime.datetime.now()
    time_txt("wordcount", 2563, init_time, end_time)

    #Word Count
    #Change Confs on .properties
    change_prop(i, "wordcount", 3, 6, 12, 3, 0, 0)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("wordcount")
    end_time = datetime.datetime.now()
    time_txt("wordcount", 36123, init_time, end_time)

    #Word Count
    #Change Confs on .properties
    change_prop(i, "wordcount", 5, 10, 12, 6, 0, 0)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("wordcount")
    end_time = datetime.datetime.now()
    time_txt("wordcount", 510126, init_time, end_time)
    
    #Spike
    #Change Confs on .properties
    change_prop(i, "spikedetection", 1, 1, 6, 3, 0, 0)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("spikedetection")
    end_time = datetime.datetime.now()
    time_txt("spikedetection", 1163, init_time, end_time)

    #Spike
    #Change Confs on .properties
    change_prop(i, "spikedetection", 1, 3, 6, 3, 0, 0)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("spikedetection")
    end_time = datetime.datetime.now()
    time_txt("spikedetection", 1363, init_time, end_time)

    #Spike
    #Change Confs on .properties
    change_prop(i, "spikedetection", 3, 6, 12, 6, 0, 0)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("spikedetection")
    end_time = datetime.datetime.now()
    time_txt("spikedetection", 36126, init_time, end_time)

    #Spike
    #Change Confs on .properties
    change_prop(i, "spikedetection", 4, 8, 8, 8, 0, 0)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("spikedetection")
    end_time = datetime.datetime.now()
    time_txt("spikedetection", 48888, init_time, end_time)

    #Spike
    #Change Confs on .properties
    change_prop(i, "spikedetection", 6, 12, 12, 8, 0, 0)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("spikedetection")
    end_time = datetime.datetime.now()
    time_txt("spikedetection", 612128, init_time, end_time)

    #Log
    #Change Confs on .properties
    change_prop(i, "logprocessing", 2, 4, 4, 1, 4, 2)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("logprocessing")
    end_time = datetime.datetime.now()
    time_txt("logprocessing", 244142, init_time, end_time)

    #Log
    #Change Confs on .properties
    change_prop(i, "logprocessing", 4, 8, 8, 2, 8, 4)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("logprocessing")
    end_time = datetime.datetime.now()
    time_txt("logprocessing", 488284, init_time, end_time)

    #Log
    #Change Confs on .properties
    change_prop(i, "logprocessing", 2, 4, 4, 8, 12, 6)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("logprocessing")
    end_time = datetime.datetime.now()
    time_txt("logprocessing", 2448126, init_time, end_time)

    #Log
    #Change Confs on .properties
    change_prop(i, "logprocessing", 6, 12, 12, 6, 12, 4)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("logprocessing")
    end_time = datetime.datetime.now()
    time_txt("logprocessing", 612126124, init_time, end_time)

    #Click
    #Change Confs on .properties
    change_prop(i, "clickanalytics", 1, 3, 6, 3, 6, 3)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("clickanalytics")
    end_time = datetime.datetime.now()
    time_txt("clickanalytics", 136363, init_time, end_time)

    #Click
    #Change Confs on .properties
    change_prop(i, "clickanalytics", 2, 4, 8, 4, 8, 4)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("clickanalytics")
    end_time = datetime.datetime.now()
    time_txt("clickanalytics", 248484, init_time, end_time)

    #Click
    #Change Confs on .properties
    change_prop(i, "clickanalytics", 4, 8, 8, 8, 8, 8)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("clickanalytics")
    end_time = datetime.datetime.now()
    time_txt("clickanalytics", 488888, init_time, end_time)

    #Click
    #Change Confs on .properties
    change_prop(i, "clickanalytics", 3, 6, 12, 6, 12, 6)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("clickanalytics")
    end_time = datetime.datetime.now()
    time_txt("clickanalytics", 36126126, init_time, end_time)

    #Click
    #Change Confs on .properties
    change_prop(i, "clickanalytics", 6, 12, 12, 12, 12, 8)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("clickanalytics")
    end_time = datetime.datetime.now()
    time_txt("clickanalytics", 6121212128, init_time, end_time)

    #sentiment
    #Change Confs on .properties
    change_prop(i, "sentimentanalysis", 4, 8, 8, 0, 0, 0)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("sentimentanalysis")
    end_time = datetime.datetime.now()
    time_txt("sentimentanalysis", 488, init_time, end_time)

    #sentiment
    #Change Confs on .properties
    change_prop(i, "sentimentanalysis", 6, 12, 12, 0, 0, 0)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("sentimentanalysis")
    end_time = datetime.datetime.now()
    time_txt("sentimentanalysis", 61212, init_time, end_time)

    #sentiment
    #Change Confs on .properties
    change_prop(i, "sentimentanalysis", 3, 6, 3, 0, 0, 0)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("sentimentanalysis")
    end_time = datetime.datetime.now()
    time_txt("sentimentanalysis", 363, init_time, end_time)

    #sentiment
    #Change Confs on .properties
    change_prop(i, "sentimentanalysis", 4, 8, 4, 0, 0, 0)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("sentimentanalysis")
    end_time = datetime.datetime.now()
    time_txt("sentimentanalysis", 484, init_time, end_time)

    #sentiment
    #Change Confs on .properties
    change_prop(i, "sentimentanalysis", 6, 12, 8, 0, 0, 0)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("sentimentanalysis")
    end_time = datetime.datetime.now()
    time_txt("sentimentanalysis", 6128, init_time, end_time)

stop_cluster()
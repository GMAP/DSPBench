import os
import time
import datetime;

exec = "stream"

def change_prop(run, app, stage1=1, stage2=1, stage3=1, stage4=1, stage5=1, stage6=1, stage7=1, stage8=1, stage9=1, stage10=1, stage11=1, stage12=1, stage13=1):
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
        os.system('echo "lp.status.sink.threads='+str(stage7)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/logprocessing.properties')
        os.system('echo "lp.country.sink.threads='+str(stage8)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/logprocessing.properties')
        os.system('echo "metrics.output=/home/gmap/metrics/'+exec+'/LP'+str(stage1)+str(stage2)+str(stage3)+str(stage4)+str(stage5)+str(stage6)+str(stage7)+str(stage8)+'/'+str(run)+'/" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/logprocessing.properties')

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
        os.system('echo "ca.location.sink.threads='+str(stage7)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/clickanalytics.properties')
        os.system('echo "metrics.output=/home/gmap/metrics/'+exec+'/CA'+str(stage1)+str(stage2)+str(stage3)+str(stage4)+str(stage5)+str(stage6)+str(stage7)+'/'+str(run)+'/" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/clickanalytics.properties')

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
    
    elif app == "frauddetection":
        #Remove last line
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/frauddetection.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/frauddetection.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/frauddetection.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/frauddetection.properties")
        #Add lines
        os.system('echo "fd.parser.threads='+str(stage1)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/frauddetection.properties')
        os.system('echo "fd.predictor.threads='+str(stage2)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/frauddetection.properties')
        os.system('echo "fd.sink.threads='+str(stage3)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/frauddetection.properties')
        os.system('echo "metrics.output=/home/gmap/metrics/'+exec+'/FD'+str(stage1)+str(stage2)+str(stage3)+'/'+str(run)+'/" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/frauddetection.properties')
    
    elif app == "machineoutlier":
        #Remove last line
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/machineoutlier.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/machineoutlier.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/machineoutlier.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/machineoutlier.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/machineoutlier.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/machineoutlier.properties")
        #Add lines
        os.system('echo "mo.parser.threads='+str(stage1)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/machineoutlier.properties')
        os.system('echo "mo.scorer.threads='+str(stage2)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/machineoutlier.properties')
        os.system('echo "mo.anomaly_scorer.threads='+str(stage3)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/machineoutlier.properties')
        os.system('echo "mo.alert_trigger.threads='+str(stage4)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/machineoutlier.properties')
        os.system('echo "mo.sink.threads='+str(stage5)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/machineoutlier.properties')
        os.system('echo "metrics.output=/home/gmap/metrics/'+exec+'/MO'+str(stage1)+str(stage2)+str(stage3)+str(stage4)+str(stage5)+'/'+str(run)+'/" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/machineoutlier.properties')
    
    elif app == "smartgrid":
        #Remove last line
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/smartgrid.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/smartgrid.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/smartgrid.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/smartgrid.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/smartgrid.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/smartgrid.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/smartgrid.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/smartgrid.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/smartgrid.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/smartgrid.properties")
        #Add lines
        os.system('echo "sg.parser.threads='+str(stage1)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/smartgrid.properties')
        os.system('echo "sg.sliding_window.threads='+str(stage2)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/smartgrid.properties')
        os.system('echo "sg.global_median.threads='+str(stage3)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/smartgrid.properties')
        os.system('echo "sg.plug_median.threads='+str(stage4)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/smartgrid.properties')
        os.system('echo "sg.outlier_detector.threads='+str(stage5)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/smartgrid.properties')
        os.system('echo "sg.house_load.threads='+str(stage6)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/smartgrid.properties')
        os.system('echo "sg.plug_load.threads='+str(stage7)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/smartgrid.properties')
        os.system('echo "sg.outlier.sink.threads='+str(stage8)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/smartgrid.properties')
        os.system('echo "sg.prediction.sink.threads='+str(stage9)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/smartgrid.properties')
        os.system('echo "metrics.output=/home/gmap/metrics/'+exec+'/SG'+str(stage1)+str(stage2)+str(stage3)+str(stage4)+str(stage5)+str(stage6)+str(stage7)+str(stage8)+str(stage9)+'/'+str(run)+'/" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/smartgrid.properties')
    
    elif app == "trafficmonitoring":  
        #Remove last line
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/trafficmonitoring.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/trafficmonitoring.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/trafficmonitoring.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/trafficmonitoring.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/trafficmonitoring.properties")
        #Add lines
        os.system('echo "tm.parser.threads='+str(stage1)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/trafficmonitoring.properties')
        os.system('echo "tm.map_matcher.threads='+str(stage2)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/trafficmonitoring.properties')
        os.system('echo "tm.speed_calculator.threads='+str(stage3)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/trafficmonitoring.properties')
        os.system('echo "tm.sink.threads='+str(stage4)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/trafficmonitoring.properties')
        os.system('echo "metrics.output=/home/gmap/metrics/'+exec+'/TM'+str(stage1)+str(stage2)+str(stage3)+str(stage4)+'/'+str(run)+'/" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/trafficmonitoring.properties')
    
    elif app == "adanalytics": 
        #Remove last line
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/adanalytics.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/adanalytics.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/adanalytics.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/adanalytics.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/adanalytics.properties")
        #Add lines
        os.system('echo "aa.click.parser.threads='+str(stage1)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/adanalytics.properties')
        os.system('echo "aa.impressions.parser.threads='+str(stage2)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/adanalytics.properties')
        os.system('echo "aa.ctr.threads='+str(stage3)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/adanalytics.properties')
        os.system('echo "aa.sink.threads='+str(stage4)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/adanalytics.properties')
        os.system('echo "metrics.output=/home/gmap/metrics/'+exec+'/AA'+str(stage1)+str(stage2)+str(stage3)+str(stage4)+'/'+str(run)+'/" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/adanalytics.properties')
    
    elif app == "bargainindex": 
        #Remove last line
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/bargainindex.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/bargainindex.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/bargainindex.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/bargainindex.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/bargainindex.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/bargainindex.properties")
        #Add lines
        os.system('echo "bi.quotes.parser.threads='+str(stage1)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/bargainindex.properties')
        os.system('echo "bi.trades.parser.threads='+str(stage2)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/bargainindex.properties')
        os.system('echo "bi.vwap.threads='+str(stage3)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/bargainindex.properties')
        os.system('echo "bi.bargainindex.threads='+str(stage4)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/bargainindex.properties')
        os.system('echo "bi.sink.threads='+str(stage5)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/bargainindex.properties')
        os.system('echo "metrics.output=/home/gmap/metrics/'+exec+'/BI'+str(stage1)+str(stage2)+str(stage3)+str(stage4)+str(stage5)+'/'+str(run)+'/" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/bargainindex.properties')
    
    elif app == "reinforcementlearner": 
        #Remove last line
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/reinforcementlearner.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/reinforcementlearner.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/reinforcementlearner.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/reinforcementlearner.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/reinforcementlearner.properties")
        #Add lines
        os.system('echo "rl.event.parser.threads='+str(stage1)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/reinforcementlearner.properties')
        os.system('echo "rl.reward.parser.threads='+str(stage2)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/reinforcementlearner.properties')
        os.system('echo "rl.learner.threads='+str(stage3)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/reinforcementlearner.properties')
        os.system('echo "rl.sink.threads='+str(stage4)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/reinforcementlearner.properties')
        os.system('echo "metrics.output=/home/gmap/metrics/'+exec+'/RL'+str(stage1)+str(stage2)+str(stage3)+str(stage4)+'/'+str(run)+'/" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/reinforcementlearner.properties')
    
    elif app == "spamfilter": 
        #Remove last line
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/spamfilter.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/spamfilter.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/spamfilter.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/spamfilter.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/spamfilter.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/spamfilter.properties")
        #Add lines
        os.system('echo "sf.training.parser.threads='+str(stage1)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/spamfilter.properties')
        os.system('echo "sf.analysis.parser.threads='+str(stage2)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/spamfilter.properties')
        os.system('echo "sf.tokenizer.threads='+str(stage3)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/spamfilter.properties')
        os.system('echo "sf.bayesrule.threads='+str(stage4)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/spamfilter.properties')
        os.system('echo "sf.sink.threads='+str(stage5)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/spamfilter.properties')
        os.system('echo "metrics.output=/home/gmap/metrics/'+exec+'/SF'+str(stage1)+str(stage2)+str(stage3)+str(stage4)+str(stage5)+'/'+str(run)+'/" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/spamfilter.properties')
    
    elif app == "trendingtopics": 
        #Remove last line
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/trendingtopics.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/trendingtopics.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/trendingtopics.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/trendingtopics.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/trendingtopics.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/trendingtopics.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/trendingtopics.properties")
        #Add lines
        os.system('echo "tt.parser.threads='+str(stage1)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/trendingtopics.properties')
        os.system('echo "tt.topic_extractor.threads='+str(stage2)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/trendingtopics.properties')
        os.system('echo "tt.counter.threads='+str(stage3)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/trendingtopics.properties')
        os.system('echo "tt.iranker.threads='+str(stage4)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/trendingtopics.properties')
        os.system('echo "tt.tranker.threads='+str(stage5)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/trendingtopics.properties')
        os.system('echo "tt.sink.threads='+str(stage6)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/trendingtopics.properties')
        os.system('echo "metrics.output=/home/gmap/metrics/'+exec+'/TT'+str(stage1)+str(stage2)+str(stage3)+str(stage4)+str(stage5)+str(stage6)+'/'+str(run)+'/" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/trendingtopics.properties')
    
    elif app == "voipstream": 
        #Remove last line
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/voipstream.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/voipstream.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/voipstream.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/voipstream.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/voipstream.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/voipstream.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/voipstream.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/voipstream.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/voipstream.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/voipstream.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/voipstream.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/voipstream.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/voipstream.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/voipstream.properties")
        #Add lines
        os.system('echo "vs.parser.threads='+str(stage1)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/voipstream.properties')
        os.system('echo "vs.vardetect.threads='+str(stage2)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/voipstream.properties')
        os.system('echo "vs.encr.threads='+str(stage3)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/voipstream.properties')
        os.system('echo "vs.ecr.threads='+str(stage4)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/voipstream.properties')
        os.system('echo "vs.rcr.threads='+str(stage5)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/voipstream.properties')
        os.system('echo "vs.ecr24.threads='+str(stage6)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/voipstream.properties')
        os.system('echo "vs.ct24.threads='+str(stage7)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/voipstream.properties')
        os.system('echo "vs.globalacd.threads='+str(stage8)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/voipstream.properties')
        os.system('echo "vs.fofir.threads='+str(stage9)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/voipstream.properties')
        os.system('echo "vs.url.threads='+str(stage10)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/voipstream.properties')
        os.system('echo "vs.acd.threads='+str(stage11)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/voipstream.properties')
        os.system('echo "vs.scorer.threads='+str(stage12)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/voipstream.properties')
        os.system('echo "vs.sink.threads='+str(stage13)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/voipstream.properties')
        os.system('echo "metrics.output=/home/gmap/metrics/'+exec+'/VS'+str(stage1)+str(stage2)+str(stage3)+str(stage4)+str(stage5)+str(stage6)+str(stage7)+str(stage8)+str(stage9)+str(stage10)+str(stage11)+str(stage12)+str(stage13)+'/'+str(run)+'/" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/voipstream.properties')
    
    elif app == "YSB":
        #Remove last line
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/YSB.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/YSB.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/YSB.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/YSB.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/YSB.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/YSB.properties")
        #Add lines
        os.system('echo "ysb.source.threads='+str(stage1)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/YSB.properties')
        os.system('echo "ysb.filter.threads='+str(stage2)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/YSB.properties')
        os.system('echo "ysb.joiner.threads='+str(stage3)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/YSB.properties')
        os.system('echo "ysb.aggregator.threads='+str(stage4)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/YSB.properties')
        os.system('echo "ysb.sink.threads='+str(stage5)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/YSB.properties')
        os.system('echo "metrics.output=/home/gmap/metrics/'+exec+'/YSB'+str(stage1)+str(stage2)+str(stage3)+str(stage4)+str(stage5)+'/'+str(run)+'/" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/YSB.properties')

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
    time.sleep(30)
    start_cluster() #os.system('~/flink-1.17.1/bin/start-cluster.sh')
    time.sleep(30)

def time_txt(app, conf, init_time, end_time):
    print("time.txt")
    with open('/home/gmap/DSPBench/dspbench-flink/txts/'+app+'-'+exec+'-'+str(conf)+'.txt', 'a') as f:
        f.write(str(init_time) + " - " + str(end_time) + "\n")

start_cluster()

for i in range(1,6):
    '''
    #Change Confs on .properties
    change_prop(i, "adanalytics", 1, 1, 1, 1)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("adanalytics")
    end_time = datetime.datetime.now()
    time_txt("adanalytics", 1111, init_time, end_time)

    #Change Confs on .properties
    change_prop(i, "adanalytics", 1, 1, 2, 1)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("adanalytics")
    end_time = datetime.datetime.now()
    time_txt("adanalytics", 1121, init_time, end_time)

    #Change Confs on .properties
    change_prop(i, "adanalytics", 2, 2, 1, 1)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("adanalytics")
    end_time = datetime.datetime.now()
    time_txt("adanalytics", 2211, init_time, end_time)

    #Change Confs on .properties
    change_prop(i, "adanalytics", 2, 2, 2, 1)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("adanalytics")
    end_time = datetime.datetime.now()
    time_txt("adanalytics", 2221, init_time, end_time)

    #Change Confs on .properties
    change_prop(i, "adanalytics", 2, 2, 4, 2)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("adanalytics")
    end_time = datetime.datetime.now()
    time_txt("adanalytics", 2242, init_time, end_time)

    #Change Confs on .properties
    change_prop(i, "adanalytics", 4, 4, 12, 8)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("adanalytics")
    end_time = datetime.datetime.now()
    time_txt("adanalytics", 44128, init_time, end_time)

    #Change Confs on .properties
    change_prop(i, "adanalytics", 4, 4, 8, 4)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("adanalytics")
    end_time = datetime.datetime.now()
    time_txt("adanalytics", 4484, init_time, end_time)

    #Change Confs on .properties
    change_prop(i, "adanalytics", 6, 6, 12, 8)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("adanalytics")
    end_time = datetime.datetime.now()
    time_txt("adanalytics", 66128, init_time, end_time)
    #######################################################################################
    #Change Confs on .properties
    change_prop(i, "bargainindex", 1, 1, 1, 1, 1)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("bargainindex")
    end_time = datetime.datetime.now()
    time_txt("bargainindex", 11111, init_time, end_time)

    #Change Confs on .properties
    change_prop(i, "bargainindex", 1, 1, 2, 1, 1)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("bargainindex")
    end_time = datetime.datetime.now()
    time_txt("bargainindex", 11211, init_time, end_time)

    #Change Confs on .properties
    change_prop(i, "bargainindex", 2, 2, 2, 2, 1)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("bargainindex")
    end_time = datetime.datetime.now()
    time_txt("bargainindex", 22221, init_time, end_time)

    #Change Confs on .properties
    change_prop(i, "bargainindex", 2, 2, 4, 2, 2)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("bargainindex")
    end_time = datetime.datetime.now()
    time_txt("bargainindex", 22422, init_time, end_time)

    #Change Confs on .properties
    change_prop(i, "bargainindex", 4, 4, 12, 8, 4)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("bargainindex")
    end_time = datetime.datetime.now()
    time_txt("bargainindex", 441284, init_time, end_time)

    #Change Confs on .properties
    change_prop(i, "bargainindex", 4, 4, 12, 8, 6)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("bargainindex")
    end_time = datetime.datetime.now()
    time_txt("bargainindex", 441286, init_time, end_time)

    #Change Confs on .properties
    change_prop(i, "bargainindex", 4, 4, 8, 4, 4)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("bargainindex")
    end_time = datetime.datetime.now()
    time_txt("bargainindex", 44844, init_time, end_time)
    #######################################################################################
    #Change Confs on .properties
    change_prop(i, "clickanalytics", 1, 1, 1, 1, 1, 1, 1)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("clickanalytics")
    end_time = datetime.datetime.now()
    time_txt("clickanalytics", 1111111, init_time, end_time)

    #Change Confs on .properties
    change_prop(i, "clickanalytics", 1, 2, 2, 2, 2, 1, 1)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("clickanalytics")
    end_time = datetime.datetime.now()
    time_txt("clickanalytics", 1222211, init_time, end_time)

    #Change Confs on .properties
    change_prop(i, "clickanalytics", 1, 2, 4, 2, 4, 2, 2)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("clickanalytics")
    end_time = datetime.datetime.now()
    time_txt("clickanalytics", 1242422, init_time, end_time)

    #Change Confs on .properties
    change_prop(i, "clickanalytics", 1, 3, 6, 3, 6, 3, 3)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("clickanalytics")
    end_time = datetime.datetime.now()
    time_txt("clickanalytics", 1363633, init_time, end_time)

    #Change Confs on .properties
    change_prop(i, "clickanalytics", 2, 4, 8, 4, 8, 4, 4)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("clickanalytics")
    end_time = datetime.datetime.now()
    time_txt("clickanalytics", 2484844, init_time, end_time)

    #Change Confs on .properties
    change_prop(i, "clickanalytics", 4, 8, 8, 8, 8, 8, 8)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("clickanalytics")
    end_time = datetime.datetime.now()
    time_txt("clickanalytics", 4888888, init_time, end_time)

    #Change Confs on .properties
    change_prop(i, "clickanalytics", 3, 6, 12, 6, 12, 6, 6)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("clickanalytics")
    end_time = datetime.datetime.now()
    time_txt("clickanalytics", 361261266, init_time, end_time)

    #Change Confs on .properties
    change_prop(i, "clickanalytics", 6, 12, 12, 12, 12, 8, 8)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("clickanalytics")
    end_time = datetime.datetime.now()
    time_txt("clickanalytics", 61212121288, init_time, end_time)
    #######################################################################################
    #Change Confs on .properties
    change_prop(i, "frauddetection", 1, 1, 1)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("frauddetection")
    end_time = datetime.datetime.now()
    time_txt("frauddetection", 111, init_time, end_time)

    #Change Confs on .properties
    change_prop(i, "frauddetection", 1, 2, 2)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("frauddetection")
    end_time = datetime.datetime.now()
    time_txt("frauddetection", 122, init_time, end_time)

    #Change Confs on .properties
    change_prop(i, "frauddetection", 1, 3, 3)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("frauddetection")
    end_time = datetime.datetime.now()
    time_txt("frauddetection", 133, init_time, end_time)

    #Change Confs on .properties
    change_prop(i, "frauddetection", 2, 4, 2)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("frauddetection")
    end_time = datetime.datetime.now()
    time_txt("frauddetection", 242, init_time, end_time)

    #Change Confs on .properties
    change_prop(i, "frauddetection", 2, 4, 4)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("frauddetection")
    end_time = datetime.datetime.now()
    time_txt("frauddetection", 244, init_time, end_time)

    #Change Confs on .properties
    change_prop(i, "frauddetection", 3, 6, 12)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("frauddetection")
    end_time = datetime.datetime.now()
    time_txt("frauddetection", 3612, init_time, end_time)

    #Change Confs on .properties
    change_prop(i, "frauddetection", 3, 6, 6)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("frauddetection")
    end_time = datetime.datetime.now()
    time_txt("frauddetection", 366, init_time, end_time)

    #Change Confs on .properties
    change_prop(i, "frauddetection", 4, 8, 4)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("frauddetection")
    end_time = datetime.datetime.now()
    time_txt("frauddetection", 484, init_time, end_time)

    #Change Confs on .properties
    change_prop(i, "frauddetection", 6, 6, 12)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("frauddetection")
    end_time = datetime.datetime.now()
    time_txt("frauddetection", 6612, init_time, end_time)

    #Change Confs on .properties
    change_prop(i, "frauddetection", 8, 8, 8)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("frauddetection")
    end_time = datetime.datetime.now()
    time_txt("frauddetection", 888, init_time, end_time)
    #######################################################################################
    #Change Confs on .properties
    change_prop(i, "logprocessing", 1, 1, 1, 1, 1, 1, 1, 1)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("logprocessing")
    end_time = datetime.datetime.now()
    time_txt("logprocessing", 11111111, init_time, end_time)

    #Change Confs on .properties
    change_prop(i, "logprocessing", 1, 2, 2, 1, 2, 2, 2, 2)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("logprocessing")
    end_time = datetime.datetime.now()
    time_txt("logprocessing", 12212222, init_time, end_time)

    #Change Confs on .properties
    change_prop(i, "logprocessing", 1, 3, 3, 1, 3, 1, 1, 1)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("logprocessing")
    end_time = datetime.datetime.now()
    time_txt("logprocessing", 13313111, init_time, end_time)

    #Change Confs on .properties
    change_prop(i, "logprocessing", 2, 4, 4, 1, 4, 2, 2, 2)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("logprocessing")
    end_time = datetime.datetime.now()
    time_txt("logprocessing", 24414222, init_time, end_time)

    #Change Confs on .properties
    change_prop(i, "logprocessing", 4, 8, 8, 2, 8, 4, 4, 4)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("logprocessing")
    end_time = datetime.datetime.now()
    time_txt("logprocessing", 48828444, init_time, end_time)

    #Change Confs on .properties
    change_prop(i, "logprocessing", 2, 4, 4, 8, 12, 6, 6, 6)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("logprocessing")
    end_time = datetime.datetime.now()
    time_txt("logprocessing", 244812666, init_time, end_time)

    #Change Confs on .properties
    change_prop(i, "logprocessing", 6, 12, 12, 6, 12, 4, 4, 4)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("logprocessing")
    end_time = datetime.datetime.now()
    time_txt("logprocessing", 61212612444, init_time, end_time)
    #######################################################################################
    #Change Confs on .properties
    change_prop(i, "machineoutlier", 1, 1, 1, 1, 1)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("machineoutlier")
    end_time = datetime.datetime.now()
    time_txt("machineoutlier", 11111, init_time, end_time)

    #Change Confs on .properties
    change_prop(i, "machineoutlier", 1, 1, 1, 1, 2)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("machineoutlier")
    end_time = datetime.datetime.now()
    time_txt("machineoutlier", 11112, init_time, end_time)

    #Change Confs on .properties
    change_prop(i, "machineoutlier", 1, 1, 1, 1, 3)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("machineoutlier")
    end_time = datetime.datetime.now()
    time_txt("machineoutlier", 11113, init_time, end_time)
    #######################################################################################
    #Change Confs on .properties
    change_prop(i, "reinforcementlearner", 1, 1, 1, 1)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("reinforcementlearner")
    end_time = datetime.datetime.now()
    time_txt("reinforcementlearner", 1111, init_time, end_time)

    #Change Confs on .properties
    change_prop(i, "reinforcementlearner", 1, 1, 2, 1)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("reinforcementlearner")
    end_time = datetime.datetime.now()
    time_txt("reinforcementlearner", 1121, init_time, end_time)

    #Change Confs on .properties
    change_prop(i, "reinforcementlearner", 2, 2, 4, 2)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("reinforcementlearner")
    end_time = datetime.datetime.now()
    time_txt("reinforcementlearner", 2242, init_time, end_time)

    #Change Confs on .properties
    change_prop(i, "reinforcementlearner", 4, 4, 8, 4)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("reinforcementlearner")
    end_time = datetime.datetime.now()
    time_txt("reinforcementlearner", 4484, init_time, end_time)

    #Change Confs on .properties
    change_prop(i, "reinforcementlearner", 6, 6, 12, 8)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("reinforcementlearner")
    end_time = datetime.datetime.now()
    time_txt("reinforcementlearner", 66128, init_time, end_time)
    #######################################################################################
    #Change Confs on .properties
    change_prop(i, "smartgrid", 1, 1, 1, 1, 1, 1, 1, 1, 1)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("smartgrid")
    end_time = datetime.datetime.now()
    time_txt("smartgrid", 111111111, init_time, end_time)

    #Change Confs on .properties
    change_prop(i, "smartgrid", 1, 1, 1, 1, 1, 1, 1, 2, 2)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("smartgrid")
    end_time = datetime.datetime.now()
    time_txt("smartgrid", 111111122, init_time, end_time)

    #Change Confs on .properties
    change_prop(i, "smartgrid", 1, 1, 1, 1, 1, 1, 1, 3, 3)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("smartgrid")
    end_time = datetime.datetime.now()
    time_txt("smartgrid", 111111133, init_time, end_time)
    #######################################################################################
    #Change Confs on .properties
    change_prop(i, "spamfilter", 1, 1, 1, 1, 1)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("spamfilter")
    end_time = datetime.datetime.now()
    time_txt("spamfilter", 11111, init_time, end_time)

    #Change Confs on .properties
    change_prop(i, "spamfilter", 1, 1, 1, 2, 1)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("spamfilter")
    end_time = datetime.datetime.now()
    time_txt("spamfilter", 11121, init_time, end_time)

    #Change Confs on .properties
    change_prop(i, "spamfilter", 2, 2, 1, 2, 1)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("spamfilter")
    end_time = datetime.datetime.now()
    time_txt("spamfilter", 22121, init_time, end_time)

    #Change Confs on .properties
    change_prop(i, "spamfilter", 2, 2, 1, 2, 2)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("spamfilter")
    end_time = datetime.datetime.now()
    time_txt("spamfilter", 22122, init_time, end_time)

    #Change Confs on .properties
    change_prop(i, "spamfilter", 3, 3, 1, 2, 2)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("spamfilter")
    end_time = datetime.datetime.now()
    time_txt("spamfilter", 33122, init_time, end_time)

    #Change Confs on .properties
    change_prop(i, "spamfilter", 3, 3, 1, 3, 1)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("spamfilter")
    end_time = datetime.datetime.now()
    time_txt("spamfilter", 33131, init_time, end_time)

    #Change Confs on .properties
    change_prop(i, "spamfilter", 4, 4, 1, 2, 1)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("spamfilter")
    end_time = datetime.datetime.now()
    time_txt("spamfilter", 44121, init_time, end_time)

    #Change Confs on .properties
    change_prop(i, "spamfilter", 4, 4, 1, 4, 2)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("spamfilter")
    end_time = datetime.datetime.now()
    time_txt("spamfilter", 44142, init_time, end_time)
    #######################################################################################
    #Change Confs on .properties
    change_prop(i, "spikedetection", 1, 1, 1, 1)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("spikedetection")
    end_time = datetime.datetime.now()
    time_txt("spikedetection", 1111, init_time, end_time)

    #Change Confs on .properties
    change_prop(i, "spikedetection", 1, 1, 2, 2)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("spikedetection")
    end_time = datetime.datetime.now()
    time_txt("spikedetection", 1122, init_time, end_time)

    #Change Confs on .properties
    change_prop(i, "spikedetection", 1, 1, 4, 2)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("spikedetection")
    end_time = datetime.datetime.now()
    time_txt("spikedetection", 1142, init_time, end_time)

    #Change Confs on .properties
    change_prop(i, "spikedetection", 1, 1, 6, 3)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("spikedetection")
    end_time = datetime.datetime.now()
    time_txt("spikedetection", 1163, init_time, end_time)

    #Change Confs on .properties
    change_prop(i, "spikedetection", 1, 3, 6, 3)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("spikedetection")
    end_time = datetime.datetime.now()
    time_txt("spikedetection", 1363, init_time, end_time)

    #Change Confs on .properties
    change_prop(i, "spikedetection", 3, 6, 12, 6)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("spikedetection")
    end_time = datetime.datetime.now()
    time_txt("spikedetection", 36126, init_time, end_time)

    #Change Confs on .properties
    change_prop(i, "spikedetection", 4, 8, 8, 8)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("spikedetection")
    end_time = datetime.datetime.now()
    time_txt("spikedetection", 4888, init_time, end_time)

    #Change Confs on .properties
    change_prop(i, "spikedetection", 6, 12, 12, 8)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("spikedetection")
    end_time = datetime.datetime.now()
    time_txt("spikedetection", 612128, init_time, end_time)
    #######################################################################################
    #Change Confs on .properties
    change_prop(i, "trafficmonitoring", 1, 1, 1, 1)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("trafficmonitoring")
    end_time = datetime.datetime.now()
    time_txt("trafficmonitoring", 1111, init_time, end_time)

    #Change Confs on .properties
    change_prop(i, "trafficmonitoring", 1, 2, 2, 1)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("trafficmonitoring")
    end_time = datetime.datetime.now()
    time_txt("trafficmonitoring", 1221, init_time, end_time)

    #Change Confs on .properties
    change_prop(i, "trafficmonitoring", 1, 2, 4, 2)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("trafficmonitoring")
    end_time = datetime.datetime.now()
    time_txt("trafficmonitoring", 1242, init_time, end_time)

    #Change Confs on .properties
    change_prop(i, "trafficmonitoring", 1, 3, 3, 1)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("trafficmonitoring")
    end_time = datetime.datetime.now()
    time_txt("trafficmonitoring", 1331, init_time, end_time)

    #Change Confs on .properties
    change_prop(i, "trafficmonitoring", 1, 3, 6, 3)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("trafficmonitoring")
    end_time = datetime.datetime.now()
    time_txt("trafficmonitoring", 1363, init_time, end_time)

    #Change Confs on .properties
    change_prop(i, "trafficmonitoring", 2, 4, 4, 4)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("trafficmonitoring")
    end_time = datetime.datetime.now()
    time_txt("trafficmonitoring", 2444, init_time, end_time)

    #Change Confs on .properties
    change_prop(i, "trafficmonitoring", 2, 4, 8, 4)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("trafficmonitoring")
    end_time = datetime.datetime.now()
    time_txt("trafficmonitoring", 2484, init_time, end_time)

    #Change Confs on .properties
    change_prop(i, "trafficmonitoring", 3, 6, 12, 6)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("trafficmonitoring")
    end_time = datetime.datetime.now()
    time_txt("trafficmonitoring", 36126, init_time, end_time)

    #Change Confs on .properties
    change_prop(i, "trafficmonitoring", 3, 6, 6, 3)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("trafficmonitoring")
    end_time = datetime.datetime.now()
    time_txt("trafficmonitoring", 3663, init_time, end_time)

    #Change Confs on .properties
    change_prop(i, "trafficmonitoring", 4, 4, 8, 4)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("trafficmonitoring")
    end_time = datetime.datetime.now()
    time_txt("trafficmonitoring", 4484, init_time, end_time)

    #Change Confs on .properties
    change_prop(i, "trafficmonitoring", 4, 4, 8, 8)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("trafficmonitoring")
    end_time = datetime.datetime.now()
    time_txt("trafficmonitoring", 4488, init_time, end_time)

    #Change Confs on .properties
    change_prop(i, "trafficmonitoring", 6, 6, 12, 6)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("trafficmonitoring")
    end_time = datetime.datetime.now()
    time_txt("trafficmonitoring", 66126, init_time, end_time)

    #Change Confs on .properties
    change_prop(i, "trafficmonitoring", 8, 8, 12, 12)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("trafficmonitoring")
    end_time = datetime.datetime.now()
    time_txt("trafficmonitoring", 881212, init_time, end_time)

    #Change Confs on .properties
    change_prop(i, "trafficmonitoring", 8, 8, 12, 8)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("trafficmonitoring")
    end_time = datetime.datetime.now()
    time_txt("trafficmonitoring", 88128, init_time, end_time)

    #Change Confs on .properties
    change_prop(i, "trafficmonitoring", 8, 8, 8, 8)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("trafficmonitoring")
    end_time = datetime.datetime.now()
    time_txt("trafficmonitoring", 8888, init_time, end_time)
    #######################################################################################
    #Change Confs on .properties
    change_prop(i, "sentimentanalysis", 1, 1, 1)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("sentimentanalysis")
    end_time = datetime.datetime.now()
    time_txt("sentimentanalysis", 111, init_time, end_time)

    #Change Confs on .properties
    change_prop(i, "sentimentanalysis", 1, 2, 2)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("sentimentanalysis")
    end_time = datetime.datetime.now()
    time_txt("sentimentanalysis", 122, init_time, end_time)

    #Change Confs on .properties
    change_prop(i, "sentimentanalysis", 2, 4, 4)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("sentimentanalysis")
    end_time = datetime.datetime.now()
    time_txt("sentimentanalysis", 244, init_time, end_time)

    #Change Confs on .properties
    change_prop(i, "sentimentanalysis", 4, 8, 8)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("sentimentanalysis")
    end_time = datetime.datetime.now()
    time_txt("sentimentanalysis", 488, init_time, end_time)

    #Change Confs on .properties
    change_prop(i, "sentimentanalysis", 6, 12, 12)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("sentimentanalysis")
    end_time = datetime.datetime.now()
    time_txt("sentimentanalysis", 61212, init_time, end_time)

    #Change Confs on .properties
    change_prop(i, "sentimentanalysis", 3, 6, 3)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("sentimentanalysis")
    end_time = datetime.datetime.now()
    time_txt("sentimentanalysis", 363, init_time, end_time)

    #Change Confs on .properties
    change_prop(i, "sentimentanalysis", 4, 8, 4)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("sentimentanalysis")
    end_time = datetime.datetime.now()
    time_txt("sentimentanalysis", 484, init_time, end_time)

    #Change Confs on .properties
    change_prop(i, "sentimentanalysis", 6, 12, 8)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("sentimentanalysis")
    end_time = datetime.datetime.now()
    time_txt("sentimentanalysis", 6128, init_time, end_time)
    #######################################################################################
    #Change Confs on .properties
    change_prop(i, "trendingtopics", 1, 1, 1, 1, 1, 1)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("trendingtopics")
    end_time = datetime.datetime.now()
    time_txt("trendingtopics", 111111, init_time, end_time)

    #Change Confs on .properties
    change_prop(i, "trendingtopics", 2, 2, 1, 1, 1, 1)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("trendingtopics")
    end_time = datetime.datetime.now()
    time_txt("trendingtopics", 221111, init_time, end_time)

    #Change Confs on .properties
    change_prop(i, "trendingtopics", 2, 2, 1, 1, 1, 2)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("trendingtopics")
    end_time = datetime.datetime.now()
    time_txt("trendingtopics", 221112, init_time, end_time)

    #Change Confs on .properties
    change_prop(i, "trendingtopics", 2, 2, 2, 2, 1, 2)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("trendingtopics")
    end_time = datetime.datetime.now()
    time_txt("trendingtopics", 222212, init_time, end_time)

    #Change Confs on .properties
    change_prop(i, "trendingtopics", 4, 2, 2, 2, 1, 2)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("trendingtopics")
    end_time = datetime.datetime.now()
    time_txt("trendingtopics", 422212, init_time, end_time)

    #Change Confs on .properties
    change_prop(i, "trendingtopics", 4, 4, 2, 2, 1, 2)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("trendingtopics")
    end_time = datetime.datetime.now()
    time_txt("trendingtopics", 442212, init_time, end_time)

    #Change Confs on .properties
    change_prop(i, "trendingtopics", 4, 4, 4, 4, 1, 2)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("trendingtopics")
    end_time = datetime.datetime.now()
    time_txt("trendingtopics", 444412, init_time, end_time)

    #Change Confs on .properties
    change_prop(i, "trendingtopics", 8, 4, 4, 4, 1, 2)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("trendingtopics")
    end_time = datetime.datetime.now()
    time_txt("trendingtopics", 844412, init_time, end_time)

    #Change Confs on .properties
    change_prop(i, "trendingtopics", 8, 6, 4, 4, 1, 2)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("trendingtopics")
    end_time = datetime.datetime.now()
    time_txt("trendingtopics", 864412, init_time, end_time)

    #Change Confs on .properties
    change_prop(i, "trendingtopics", 8, 6, 4, 4, 1, 4)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("trendingtopics")
    end_time = datetime.datetime.now()
    time_txt("trendingtopics", 864414, init_time, end_time)
    #######################################################################################
    #Change Confs on .properties
    change_prop(i, "voipstream", 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("voipstream")
    end_time = datetime.datetime.now()
    time_txt("voipstream", 1111111111111, init_time, end_time)

    #Change Confs on .properties
    change_prop(i, "voipstream", 1, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("voipstream")
    end_time = datetime.datetime.now()
    time_txt("voipstream", 1211111111111, init_time, end_time)

    #Change Confs on .properties
    change_prop(i, "voipstream", 1, 2, 2, 2, 2, 2, 2, 2, 1, 1, 1, 1, 1)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("voipstream")
    end_time = datetime.datetime.now()
    time_txt("voipstream", 1222222211111, init_time, end_time)

    #Change Confs on .properties
    change_prop(i, "voipstream", 2, 4, 2, 2, 2, 2, 2, 2, 1, 1, 1, 1, 1)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("voipstream")
    end_time = datetime.datetime.now()
    time_txt("voipstream", 2422222211111, init_time, end_time)

    #Change Confs on .properties
    change_prop(i, "voipstream", 2, 4, 4, 4, 4, 4, 4, 4, 2, 2, 2, 1, 1)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("voipstream")
    end_time = datetime.datetime.now()
    time_txt("voipstream", 2444444422211, init_time, end_time)

    #Change Confs on .properties
    change_prop(i, "voipstream", 4, 8, 4, 4, 4, 4, 4, 4, 2, 2, 2, 1, 1)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("voipstream")
    end_time = datetime.datetime.now()
    time_txt("voipstream", 4844444422211, init_time, end_time)

    #Change Confs on .properties
    change_prop(i, "voipstream", 4, 8, 8, 8, 8, 8, 8, 8, 4, 4, 4, 2, 2)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("voipstream")
    end_time = datetime.datetime.now()
    time_txt("voipstream", 488888844422, init_time, end_time)
    #######################################################################################
    #Change Confs on .properties
    change_prop(i, "wordcount", 1, 1, 1, 1)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("wordcount")
    end_time = datetime.datetime.now()
    time_txt("wordcount", 1111, init_time, end_time)

    #Change Confs on .properties
    change_prop(i, "wordcount", 1, 2, 2, 2)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("wordcount")
    end_time = datetime.datetime.now()
    time_txt("wordcount", 1222, init_time, end_time)

    #Change Confs on .properties
    change_prop(i, "wordcount", 1, 3, 6, 3)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("wordcount")
    end_time = datetime.datetime.now()
    time_txt("wordcount", 1363, init_time, end_time)

    #Change Confs on .properties
    change_prop(i, "wordcount", 2, 4, 4, 4)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("wordcount")
    end_time = datetime.datetime.now()
    time_txt("wordcount", 2444, init_time, end_time)

    #Change Confs on .properties
    change_prop(i, "wordcount", 2, 5, 6, 3)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("wordcount")
    end_time = datetime.datetime.now()
    time_txt("wordcount", 2563, init_time, end_time)

    #Change Confs on .properties
    change_prop(i, "wordcount", 3, 6, 12, 3)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("wordcount")
    end_time = datetime.datetime.now()
    time_txt("wordcount", 36123, init_time, end_time)

    #Change Confs on .properties
    change_prop(i, "wordcount", 5, 10, 12, 6)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("wordcount")
    end_time = datetime.datetime.now()
    time_txt("wordcount", 510126, init_time, end_time)
    '''
    
    #Change Confs on .properties
    change_prop(i, "wordcount", 1, 1, 1, 1)
    restart_cluster()
    init_time = datetime.datetime.now()
    start_job("wordcount")
    end_time = datetime.datetime.now()
    time_txt("wordcount", 1111, init_time, end_time)

    change_prop(i, "wordcount", 1, 2, 1, 1)
    restart_cluster()
    init_time = datetime.datetime.now()
    start_job("wordcount")
    end_time = datetime.datetime.now()
    time_txt("wordcount", 1211, init_time, end_time)

    change_prop(i, "wordcount", 2, 4, 1, 1)
    restart_cluster()
    init_time = datetime.datetime.now()
    start_job("wordcount")
    end_time = datetime.datetime.now()
    time_txt("wordcount", 2411, init_time, end_time)

    change_prop(i, "wordcount", 4, 6, 2, 1)
    restart_cluster()
    init_time = datetime.datetime.now()
    start_job("wordcount")
    end_time = datetime.datetime.now()
    time_txt("wordcount", 4621, init_time, end_time)

    change_prop(i, "wordcount", 6, 6, 6, 2)
    restart_cluster()
    init_time = datetime.datetime.now()
    start_job("wordcount")
    end_time = datetime.datetime.now()
    time_txt("wordcount", 6662, init_time, end_time)
    #########################################################
    #Change Confs on .properties
    change_prop(i, "spikedetection", 1, 1, 1, 1)
    restart_cluster()
    init_time = datetime.datetime.now()
    start_job("spikedetection")
    end_time = datetime.datetime.now()
    time_txt("spikedetection", 1111, init_time, end_time)

    change_prop(i, "spikedetection", 1, 2, 1, 1)
    restart_cluster()
    init_time = datetime.datetime.now()
    start_job("spikedetection")
    end_time = datetime.datetime.now()
    time_txt("spikedetection", 1211, init_time, end_time)

    change_prop(i, "spikedetection", 2, 4, 1, 1)
    restart_cluster()
    init_time = datetime.datetime.now()
    start_job("spikedetection")
    end_time = datetime.datetime.now()
    time_txt("spikedetection", 2411, init_time, end_time)

    change_prop(i, "spikedetection", 4, 6, 2, 1)
    restart_cluster()
    init_time = datetime.datetime.now()
    start_job("spikedetection")
    end_time = datetime.datetime.now()
    time_txt("spikedetection", 4621, init_time, end_time)

    change_prop(i, "spikedetection", 6, 6, 6, 2)
    restart_cluster()
    init_time = datetime.datetime.now()
    start_job("spikedetection")
    end_time = datetime.datetime.now()
    time_txt("spikedetection", 6662, init_time, end_time)
    #############################################################
    change_prop(i, "sentimentanalysis", 1, 1, 1)
    restart_cluster()
    init_time = datetime.datetime.now()
    start_job("sentimentanalysis")
    end_time = datetime.datetime.now()
    time_txt("sentimentanalysis", 111, init_time, end_time)

    change_prop(i, "sentimentanalysis", 1, 2, 1)
    restart_cluster()
    init_time = datetime.datetime.now()
    start_job("sentimentanalysis")
    end_time = datetime.datetime.now()
    time_txt("sentimentanalysis", 121, init_time, end_time)

    change_prop(i, "sentimentanalysis", 2, 4, 1)
    restart_cluster()
    init_time = datetime.datetime.now()
    start_job("sentimentanalysis")
    end_time = datetime.datetime.now()
    time_txt("sentimentanalysis", 241, init_time, end_time)

    change_prop(i, "sentimentanalysis", 4, 6, 2)
    restart_cluster()
    init_time = datetime.datetime.now()
    start_job("sentimentanalysis")
    end_time = datetime.datetime.now()
    time_txt("sentimentanalysis", 462, init_time, end_time)

    change_prop(i, "sentimentanalysis", 6, 6, 4)
    restart_cluster()
    init_time = datetime.datetime.now()
    start_job("sentimentanalysis")
    end_time = datetime.datetime.now()
    time_txt("sentimentanalysis", 664, init_time, end_time)

    change_prop(i, "sentimentanalysis", 8, 6, 6)
    restart_cluster()
    init_time = datetime.datetime.now()
    start_job("sentimentanalysis")
    end_time = datetime.datetime.now()
    time_txt("sentimentanalysis", 866, init_time, end_time)
    ##########################################################
    change_prop(i, "YSB", 1, 1, 1, 1, 1)
    restart_cluster()
    init_time = datetime.datetime.now()
    start_job("YSB")
    end_time = datetime.datetime.now()
    time_txt("YSB", 11111, init_time, end_time)

    change_prop(i, "YSB", 1, 2, 1, 1, 1)
    restart_cluster()
    init_time = datetime.datetime.now()
    start_job("YSB")
    end_time = datetime.datetime.now()
    time_txt("YSB", 12111, init_time, end_time)

    change_prop(i, "YSB", 2, 4, 1, 1, 1)
    restart_cluster()
    init_time = datetime.datetime.now()
    start_job("YSB")
    end_time = datetime.datetime.now()
    time_txt("YSB", 24111, init_time, end_time)

    change_prop(i, "YSB", 4, 6, 2, 1, 1)
    restart_cluster()
    init_time = datetime.datetime.now()
    start_job("YSB")
    end_time = datetime.datetime.now()
    time_txt("YSB", 46211, init_time, end_time)

    change_prop(i, "YSB", 6, 6, 6, 1, 1)
    restart_cluster()
    init_time = datetime.datetime.now()
    start_job("YSB")
    end_time = datetime.datetime.now()
    time_txt("YSB", 66611, init_time, end_time)
    
stop_cluster()

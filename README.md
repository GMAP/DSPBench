# # <img alt="dspbench" src="https://raw.githubusercontent.com/GMAP/DSPBench/master/img/logo2.png" width="300">

DSPBench - Data Stream Processing Benchmark Suite and Framework

## Basic Concepts

- Stream: a communication channel that connects operators
- Operator: can be a source of tuples, a sink or a normal operator with input and
  output streams.
- Tuple: the unit of data to be processed by an operator and transported through a stream.
- Task: the wiring of operators and streams, e.g. the DAG.

## Components



## <a id="applications"></a>Applications and Datasets

| Application Name      | Prefix | Sample Data              | Dataset
|-----------------------|--------|--------------------------|--------
| ads-analytics         | ad     | [ad-clicks.dat][13]      | [KDD Cup 2012][8] (12GB)
| bargain-index         | bi     | [stocks.csv][27]         | [Kaggle Stock Market Dataset of NASDAQ][26] (3GB), [Yahoo Finance][2], [Google Finance][3]
| click-analytics       | ca     | [click-stream.json][14]  | [1998 WorldCup][7] (104GB)
| fraud-detection       | fd     | [credit-card.dat][15]    | <generated>
| linear-road           | lr     |                          | <generated>
| log-processing        | lp     | [http-server.log][16]    | [1998 WorldCup][7] (104GB)
| machine-outlier       | mo     | [cluster-traces.csv][17] | [Google Cluster Traces][6] (36GB)
| reinforcement-learner | rl     |                          | <generated>
| sentiment-analysis    | sa     |                          | [Twitter Streaming][5]
| spam-filter           | sf     | [enron.json][18]         | [TREC 2007][9] (547MB, labeled)<br />[SPAM Archive][10] (~1.2GB, spam)<br />[Enron Email Dataset][11] (2.6GB, raw)<br />[Enron Spam Dataset][12] (50MB, labeled)
| spike-detection       | sd     | [sensors.dat][19]        | [Intel Berkeley Research Lab][4] (150MB)
| traffic-monitoring    | tm     | [taxi-traces.csv][22]    | [Beijing Taxi Traces][21]<br />Shapefile from: https://download.bbbike.org/osm/bbbike/
| trending-topics       | tt     |                          | [Twitter Streaming][5]
| voipstream            | vs     |                          | <generated>
| word-count            | wc     | [books.dat][20]          | [Project Gutenberg][1] (~8GB)
| smart-grid            | sg     | [smart-grid.csv][23]     | [DEBS 2014 Grand Challenge][24] (3.2GB)


## <a id="usage"></a>Usage

### <a id="build"></a>Build

```bash
$ git clone git@github.com:GMAP/DSPBench.git
$ mvn clean install package -P <profile>
```

Use the `local` profile to run the applications in local mode or `storm`, `spark` and `flink` to run in a remote cluster.


## <a id="metrics"></a>Metrics

By using hooks and the [metrics](http://metrics.codahale.com/) library it is possible to collect performance metrics of operators and sources. In operators the information is collected about the number of received and emitted tuples and the execution time, while for sources information about the complete latency and emitted tuples is recorded.
To enable metric collection, use the following configuration:

```
metrics.enabled=true
metrics.reporter=csv
metrics.interval.value=2
metrics.interval.unit=seconds
metrics.output=/tmp
```





[1]: http://www.gutenberg.org/
[2]: https://finance.yahoo.com/
[3]: https://www.google.com/finance
[4]: http://db.csail.mit.edu/labdata/labdata.html
[5]: https://dev.twitter.com/docs/api/streaming
[6]: http://code.google.com/p/googleclusterdata/
[7]: http://ita.ee.lbl.gov/html/contrib/WorldCup.html
[8]: http://www.kddcup2012.org/c/kddcup2012-track2/data
[9]: http://plg.uwaterloo.ca/~gvcormac/spam/
[10]: http://untroubled.org/spam/
[11]: http://www.cs.cmu.edu/~./enron/
[12]: http://nlp.cs.aueb.gr/software_and_datasets/Enron-Spam/index.html

[13]: data/ad-clicks.dat
[14]: data/click-stream.json
[15]: data/credit-card.dat
[16]: data/http-server.log
[17]: data/cluster-traces.csv
[18]: data/enron.json
[19]: data/sensors.dat
[20]: data/books.dat

[21]: http://anrg.usc.edu/www/downloads/
[22]: data/taxi-traces.csv
[23]: data/smart-grid.csv
[24]: https://drive.google.com/file/d/0B0TBL8JNn3JgV29HZWhSSVREQ0E/edit?usp=sharing
[25]: http://corsi.dei.polimi.it/distsys/2013-2014/projects.html

[26]: https://www.kaggle.com/jacksoncrow/stock-market-dataset

[27]: data/stocks.csv

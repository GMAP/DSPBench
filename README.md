# <img alt="dspbench" src="https://raw.githubusercontent.com/GMAP/DSPBench/master/img/logo2.png" width="300">

DSPBench - Data Stream Processing Benchmark Suite and Framework

## Basic Concepts

- Stream: a communication channel that connects operators
- Operator: can be a source of tuples, a sink or a normal operator with input and
  output streams.
- Tuple: the unit of data to be processed by an operator and transported through a stream.
- Task: the wiring of operators and streams, e.g. the DAG.

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


## <a id="configuration"></a>Configuration

Instead of each application having its own sources and sinks (bolts that send data to other systems), we have defined a few basic sources and sinks.

### <a id="configuration-spouts"></a>Sources

All but the `GeneratorSource` need a `Parser`. The parser receives a string and returns a list of values, following the schema defined in the task. To set a source that reads from a file and parses the data as a Common Log Format, the configuration file would look like this:

```
<app-prefix>.source.threads=1
<app-prefix>.source.class=io.dspbench.base.FileSource
<app-prefix>.source.path=./data/http-server.log
<app-prefix>.source.parser=io.dspbench.applications.logprocessing.CommonLogParser
```

Defalult parsers:

| Parse                    | Output Fields
|--------------------------|--------------------
| AdEventParser            | (quer_id, ad_id, event) 
| BeijingTaxiTraceParser   | (car_id, date, occ, speed, bearing, lat, lon)
| ClickStreamParser        | (ip, url, client_key)
| CommonLogParser          | (ip, timestamp, minute, request, response, byte_size)
| DublinBusTraceParser     | (car_id, date, occ, speed, bearing, lat, lon)
| GoogleTracesParser       | (timestamp, id, cpu, memory)
| JsonEmailParser          | (id, message[, is_spam])
| JsonParser               | (json_object)
| SensorParser             | (id, timestamp, value)
| SmartPlugParser          | (id, timestamp, value, property, plugId, householdId, houseId)
| StringParser             | (string)
| TransactionParser        | (event_id, actions)


#### <a id="configuration-generator-spout"></a>GeneratorSource

The `GeneratorSource` doesn't need a parser, instead it uses an instance of a class that extends the `Generator` class. Each time the generator is called it returns a new tuple.

```
<app-prefix>.spout.threads=1
<app-prefix>.spout.class=storm.applications.spout.GeneratorSpout
<app-prefix>.spout.generator=storm.applications.spout.generator.SensorGenerator
```

Defalult generators:

| Generator                | Configurations
|--------------------------|------------------------------------------------------
| CDRGenerator             | `vs.generator.population`<br />`vs.generator.error_prob`
| MachineMetadataGenerator | `mo.generator.num_machines`
| RandomSentenceGenerator  | 
| SensorGenerator          | `sd.generator.count`
| SmartPlugGenerator       | --


##### <a id="configuration-smart-plug-generator"></a>SmartPlugGenerator

The SmartPlugGenerator is an adaptation of a generator built by [Alessandro Sivieri][25]:

> Generates a dataset of a random set of smart plugs, each being part of a household, 
> which is, in turn, part of a house. Each smart plug records the actual load 
> (in Watts) at each second. The generated dataset is inspired by the DEBS 2014 
> challenge and follow a similar format, a sequence of 6 comma separated values 
> for each line (i.e., for each reading):
> 
>  - a unique identifier of the measurement [64 bit unsigned integer value]
>  - a timestamp of measurement (number of seconds since January 1, 1970, 00:00:00 GMT) [64 bit unsigned integer value]
>  - a unique identifier (within a household) of the smart plug [32 bit unsigned integer value]
>  - a unique identifier of a household (within a house) where the plug is located [32 bit unsigned integer value]
>  - a unique identifier of a house where the household with the plug is located [32 bit unsigned integer value]
>  - the measurement [32 bit unsigned integer]

This class generates smart plug readings at fixed time intervals, storing them 
into a queue that will be consumed by a `GeneratorSpout`.

The readings are generated by a separated thread and the interval resolutions is
of seconds. In order to increase the volume of readings you can decrease the 
interval down to 1 second. If you need more data volume you will have to tune
the other configuration parameters.

Configurations parameters:

  - `sg.generator.interval_seconds`: interval of record generation in seconds.
  - `sg.generator.houses.num`: number of houses in the scenario.
  - `sg.generator.households.min` and `sg.generator.households.max`: the range of number of households within a house.
  - `sg.generator.plugs.min` and `sg.generator.plugs.max`: the range of smart plugs within a household.
  - `sg.generator.load.list`: a comma-separated list of peak loads that will be randomly assigned to smart plugs.
  - `sg.generator.load.oscillation`: by how much the peak load of the smart plug will oscillate.
  - `sg.generator.on.probability`: the probability of the smart plug being on.
  - `sg.generator.on.lengths`: a comma-separated list of lengths of time to be selected from to set the amount of time that the smart plug will be on.


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

package flink.sink;

import flink.application.YSB.Aggregate_Event;
import flink.application.voipstream.CallDetailRecord;
import flink.constants.*;
import flink.tools.Rankings;
import flink.util.Metrics;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.types.BooleanValue;
import org.apache.kafka.common.errors.SerializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.Serializable;
import java.util.Date;

/**
 *
 */
public class KafkasSink extends BaseSink implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(KafkasSink.class);

    Configuration config;
    private final ObjectMapper objectMapper = new ObjectMapper();
    Metrics metrics = new Metrics();
    
    public void initialize(Configuration config) {
        metrics.initialize(config, this.getClass().getSimpleName());
        this.config = config;
    }

    @Override
    public void sinkStreamWC(DataStream<Tuple2<String, Integer>> input) {
        String prefix = "wc";
        KafkaSink<Tuple2<String, Integer>> sink = KafkaSink.<Tuple2<String, Integer>>builder()
        .setBootstrapServers(config.getString(String.format(BaseConstants.BaseConf.KAFKA_HOST, prefix),"localhost:9092"))
        .setRecordSerializer(KafkaRecordSerializationSchema.builder()
            .setTopic(config.getString(String.format(BaseConstants.BaseConf.KAFKA_SINK_TOPIC, prefix),"booksSink"))
            .setValueSerializationSchema(new SerializationSchema<Tuple2<String, Integer>>() {

                @Override
                public byte[] serialize(Tuple2<String, Integer> element) {
                    try {
                        if (element == null){
                            System.out.println("Null received at serializing");
                            return null;
                        }
                        System.out.println("Serializing...");
                        return objectMapper.writeValueAsBytes(element.f0 + "," + element.f1.toString());
                    } catch (Exception e) {
                        throw new SerializationException("Error when serializing Message to byte[]");
                    } 
                }
            })
            .build()
        )
        .build();
        input.sinkTo(sink).setParallelism(config.getInteger(WordCountConstants.Conf.SINK_THREADS, 1));
    }

    @Override
    public void sinkStreamTM(DataStream<Tuple4<Date, Integer, Integer, Integer>> input) {
        String prefix = "tm";
        KafkaSink<Tuple4<Date, Integer, Integer, Integer>> sink = KafkaSink.<Tuple4<Date, Integer, Integer, Integer>>builder()
        .setBootstrapServers(config.getString(String.format(BaseConstants.BaseConf.KAFKA_HOST, prefix),"localhost:9092"))
        .setRecordSerializer(KafkaRecordSerializationSchema.builder()
            .setTopic(config.getString(String.format(BaseConstants.BaseConf.KAFKA_SINK_TOPIC, prefix),"trafficSink"))
            .setValueSerializationSchema(new SerializationSchema<Tuple4<Date, Integer, Integer, Integer>>() {

                @Override
                public byte[] serialize(Tuple4<Date, Integer, Integer, Integer> element) {
                    try {
                        if (element == null){
                            System.out.println("Null received at serializing");
                            return null;
                        }
                        System.out.println("Serializing...");
                        return objectMapper.writeValueAsBytes(element.f0.toString() + "," + element.f1.toString() + "," + element.f2.toString() + "," + element.f3.toString());
                    } catch (Exception e) {
                        throw new SerializationException("Error when serializing Message to byte[]");
                    } 
                }
            })
            .build()
        )
        .build();
        input.sinkTo(sink).setParallelism(config.getInteger(TrafficMonitoringConstants.Conf.SINK_THREADS, 1));
    }

    @Override
    public void sinkStreamSD(DataStream<Tuple4<String, Double, Double, String>> input) {
        String prefix = "sd";
        KafkaSink<Tuple4<String, Double, Double, String>> sink = KafkaSink.<Tuple4<String, Double, Double, String>>builder()
        .setBootstrapServers(config.getString(String.format(BaseConstants.BaseConf.KAFKA_HOST, prefix),"localhost:9092"))
        .setRecordSerializer(KafkaRecordSerializationSchema.builder()
            .setTopic(config.getString(String.format(BaseConstants.BaseConf.KAFKA_SINK_TOPIC, prefix),"spikeSink"))
            .setValueSerializationSchema(new SerializationSchema<Tuple4<String, Double, Double, String>>() {

                @Override
                public byte[] serialize(Tuple4<String, Double, Double, String> element) {
                    try {
                        if (element == null){
                            System.out.println("Null received at serializing");
                            return null;
                        }
                        System.out.println("Serializing...");
                        return objectMapper.writeValueAsBytes(element.f0 + "," + element.f1.toString() + "," + element.f2.toString()+ "," + element.f3);
                    } catch (Exception e) {
                        throw new SerializationException("Error when serializing Message to byte[]");
                    } 
                }
            })
            .build()
        )
        .build();
        input.sinkTo(sink).setParallelism(config.getInteger(SpikeDetectionConstants.Conf.SINK_THREADS, 1));
    }

    @Override
    public void sinkStreamSA(DataStream<Tuple5<String, String, Date, String, Double>> input) {
        String prefix = "sa";
        KafkaSink<Tuple5<String, String, Date, String, Double>> sink = KafkaSink.<Tuple5<String, String, Date, String, Double>>builder()
        .setBootstrapServers(config.getString(String.format(BaseConstants.BaseConf.KAFKA_HOST, prefix),"localhost:9092"))
        .setRecordSerializer(KafkaRecordSerializationSchema.builder()
            .setTopic(config.getString(String.format(BaseConstants.BaseConf.KAFKA_SINK_TOPIC, prefix),"sentimentSink"))
            .setValueSerializationSchema(new SerializationSchema<Tuple5<String, String, Date, String, Double>>() {

                @Override
                public byte[] serialize(Tuple5<String, String, Date, String, Double> element) {
                    try {
                        if (element == null){
                            System.out.println("Null received at serializing");
                            return null;
                        }
                        System.out.println("Serializing...");
                        return objectMapper.writeValueAsBytes(element.f0 + "," + element.f1 + "," + element.f2.toString() + "," + element.f3 + "," + element.f4.toString());
                    } catch (Exception e) {
                        throw new SerializationException("Error when serializing Message to byte[]");
                    } 
                }
            })
            .build()
        )
        .build();
        input.sinkTo(sink).setParallelism(config.getInteger(SentimentAnalysisConstants.Conf.SINK_THREADS, 1));
    }

    @Override
    public void sinkStreamMO(DataStream<Tuple5<String, Double, Long, Boolean, Object>> input) {
        String prefix = "mo";
        KafkaSink<Tuple5<String, Double, Long, Boolean, Object>> sink = KafkaSink.<Tuple5<String, Double, Long, Boolean, Object>>builder()
        .setBootstrapServers(config.getString(String.format(BaseConstants.BaseConf.KAFKA_HOST, prefix),"localhost:9092"))
        .setRecordSerializer(KafkaRecordSerializationSchema.builder()
            .setTopic(config.getString(String.format(BaseConstants.BaseConf.KAFKA_SINK_TOPIC, prefix),"machinesSink"))
            .setValueSerializationSchema(new SerializationSchema<Tuple5<String, Double, Long, Boolean, Object>>() {

                @Override
                public byte[] serialize(Tuple5<String, Double, Long, Boolean, Object> element) {
                    try {
                        if (element == null){
                            System.out.println("Null received at serializing");
                            return null;
                        }
                        System.out.println("Serializing...");
                        return objectMapper.writeValueAsBytes(element.f0 + "," + element.f1.toString() + "," + element.f2.toString() + "," + element.f3.toString() + "," + element.f4.toString());
                    } catch (Exception e) {
                        throw new SerializationException("Error when serializing Message to byte[]");
                    } 
                }
            })
            .build()
        )
        .build();
        input.sinkTo(sink).setParallelism(config.getInteger(MachineOutlierConstants.Conf.SINK_THREADS, 1));
    }

    @Override
    public void sinkStreamFD(DataStream<Tuple3<String, Double, String>> input) {
        String prefix = "fd";
        KafkaSink<Tuple3<String, Double, String>> sink = KafkaSink.<Tuple3<String, Double, String>>builder()
        .setBootstrapServers(config.getString(String.format(BaseConstants.BaseConf.KAFKA_HOST, prefix),"localhost:9092"))
        .setRecordSerializer(KafkaRecordSerializationSchema.builder()
            .setTopic(config.getString(String.format(BaseConstants.BaseConf.KAFKA_SINK_TOPIC, prefix),"fraudsSink"))
            .setValueSerializationSchema(new SerializationSchema<Tuple3<String, Double, String>>() {

                @Override
                public byte[] serialize(Tuple3<String, Double, String> element) {
                    try {
                        if (element == null){
                            System.out.println("Null received at serializing");
                            return null;
                        }
                        System.out.println("Serializing...");
                        return objectMapper.writeValueAsBytes(element.f0 + "," + element.f1.toString() + "," + element.f2);
                    } catch (Exception e) {
                        throw new SerializationException("Error when serializing Message to byte[]");
                    } 
                }
            })
            .build()
        )
        .build();
        input.sinkTo(sink).setParallelism(config.getInteger(FraudDetectionConstants.Conf.SINK_THREADS, 1));
    }

    @Override
    public void sinkStreamSGOutlier(DataStream<Tuple4<Long, Long, String, Double>> input) {
        String prefix = "sg";
        KafkaSink<Tuple4<Long, Long, String, Double>> sink = KafkaSink.<Tuple4<Long, Long, String, Double>>builder()
        .setBootstrapServers(config.getString(String.format(BaseConstants.BaseConf.KAFKA_HOST, prefix),"localhost:9092"))
        .setRecordSerializer(KafkaRecordSerializationSchema.builder()
            .setTopic(config.getString(String.format(BaseConstants.BaseConf.KAFKA_SINK_TOPIC, prefix+".outlier"),"gridsOutlierSink"))
            .setValueSerializationSchema(new SerializationSchema<Tuple4<Long, Long, String, Double>>() {

                @Override
                public byte[] serialize(Tuple4<Long, Long, String, Double> element) {
                    try {
                        if (element == null){
                            System.out.println("Null received at serializing");
                            return null;
                        }
                        System.out.println("Serializing...");
                        return objectMapper.writeValueAsBytes(element.f0.toString() + "," + element.f1.toString() + "," + element.f2 + "," + element.f3.toString());
                    } catch (Exception e) {
                        throw new SerializationException("Error when serializing Message to byte[]");
                    } 
                }
            })
            .build()
        )
        .build();
        input.sinkTo(sink).setParallelism(config.getInteger(SmartGridConstants.Conf.OUTLIER_SINK_THREADS, 1));
    }

    @Override
    public void sinkStreamSGHouse(DataStream<Tuple3<Long, String, Double>> input) {
        String prefix = "sg";
        KafkaSink<Tuple3<Long, String, Double>> sink = KafkaSink.<Tuple3<Long, String, Double>>builder()
        .setBootstrapServers(config.getString(String.format(BaseConstants.BaseConf.KAFKA_HOST, prefix),"localhost:9092"))
        .setRecordSerializer(KafkaRecordSerializationSchema.builder()
            .setTopic(config.getString(String.format(BaseConstants.BaseConf.KAFKA_SINK_TOPIC, prefix+".prediction"),"gridsPredictionSink"))
            .setValueSerializationSchema(new SerializationSchema<Tuple3<Long, String, Double>>() {

                @Override
                public byte[] serialize(Tuple3<Long, String, Double> element) {
                    try {
                        if (element == null){
                            System.out.println("Null received at serializing");
                            return null;
                        }
                        System.out.println("Serializing...");
                        return objectMapper.writeValueAsBytes("House," + element.f0.toString() + "," + element.f1 + "," + element.f2.toString());
                    } catch (Exception e) {
                        throw new SerializationException("Error when serializing Message to byte[]");
                    } 
                }
            })
            .build()
        )
        .build();
        input.sinkTo(sink).setParallelism(config.getInteger(SmartGridConstants.Conf.PREDICTION_SINK_THREADS, 1));
    }

    @Override
    public void sinkStreamSGPlug(DataStream<Tuple5<Long, String, String, String, Double>> input) {
        String prefix = "sg";
        KafkaSink<Tuple5<Long, String, String, String, Double>> sink = KafkaSink.<Tuple5<Long, String, String, String, Double>>builder()
        .setBootstrapServers(config.getString(String.format(BaseConstants.BaseConf.KAFKA_HOST, prefix),"localhost:9092"))
        .setRecordSerializer(KafkaRecordSerializationSchema.builder()
            .setTopic(config.getString(String.format(BaseConstants.BaseConf.KAFKA_SINK_TOPIC, prefix+".prediction"),"gridsPredictionSink"))
            .setValueSerializationSchema(new SerializationSchema<Tuple5<Long, String, String, String, Double>>() {

                @Override
                public byte[] serialize(Tuple5<Long, String, String, String, Double> element) {
                    try {
                        if (element == null){
                            System.out.println("Null received at serializing");
                            return null;
                        }
                        System.out.println("Serializing...");
                        return objectMapper.writeValueAsBytes("Plug," + element.f0.toString() + "," + element.f1 + "," + element.f2 + "," + element.f3 + "," + element.f4.toString());
                    } catch (Exception e) {
                        throw new SerializationException("Error when serializing Message to byte[]");
                    } 
                }
            })
            .build()
        )
        .build();
        input.sinkTo(sink).setParallelism(config.getInteger(SmartGridConstants.Conf.PREDICTION_SINK_THREADS, 1));
    }

    @Override
    public void createSinkLPVol(DataStream<Tuple2<Long, Long>> input) {
        String prefix = "lp";
        KafkaSink<Tuple2<Long, Long>> sink = KafkaSink.<Tuple2<Long, Long>>builder()
        .setBootstrapServers(config.getString(String.format(BaseConstants.BaseConf.KAFKA_HOST, prefix),"localhost:9092"))
        .setRecordSerializer(KafkaRecordSerializationSchema.builder()
            .setTopic(config.getString(String.format(BaseConstants.BaseConf.KAFKA_SINK_TOPIC, prefix+".count"),"logVolumeSink"))
            .setValueSerializationSchema(new SerializationSchema<Tuple2<Long, Long>>() {

                @Override
                public byte[] serialize(Tuple2<Long, Long> element) {
                    try {
                        if (element == null){
                            System.out.println("Null received at serializing");
                            return null;
                        }
                        System.out.println("Serializing...");
                        return objectMapper.writeValueAsBytes(element.f0.toString() + "," + element.f1.toString());
                    } catch (Exception e) {
                        throw new SerializationException("Error when serializing Message to byte[]");
                    } 
                }
            })
            .build()
        )
        .build();
        input.sinkTo(sink).setParallelism(config.getInteger(LogProcessingConstants.Conf.VOLUME_SINK_THREADS, 1));
    }

    @Override
    public void createSinkLPStatus(DataStream<Tuple2<Integer, Integer>> input) {
        String prefix = "lp";
        KafkaSink<Tuple2<Integer, Integer>> sink = KafkaSink.<Tuple2<Integer, Integer>>builder()
        .setBootstrapServers(config.getString(String.format(BaseConstants.BaseConf.KAFKA_HOST, prefix),"localhost:9092"))
        .setRecordSerializer(KafkaRecordSerializationSchema.builder()
            .setTopic(config.getString(String.format(BaseConstants.BaseConf.KAFKA_SINK_TOPIC, prefix+".status"),"logStatusSink"))
            .setValueSerializationSchema(new SerializationSchema<Tuple2<Integer, Integer>>() {

                @Override
                public byte[] serialize(Tuple2<Integer, Integer> element) {
                    try {
                        if (element == null){
                            System.out.println("Null received at serializing");
                            return null;
                        }
                        System.out.println("Serializing...");
                        return objectMapper.writeValueAsBytes(element.f0.toString() + "," + element.f1.toString());
                    } catch (Exception e) {
                        throw new SerializationException("Error when serializing Message to byte[]");
                    } 
                }
            })
            .build()
        )
        .build();
        input.sinkTo(sink).setParallelism(config.getInteger(LogProcessingConstants.Conf.STATUS_SINK_THREADS, 1));
    }

    @Override
    public void createSinkLPGeo(DataStream<Tuple4<String, Integer, String, Integer>> input) {
        String prefix = "lp";
        KafkaSink<Tuple4<String, Integer, String, Integer>> sink = KafkaSink.<Tuple4<String, Integer, String, Integer>>builder()
        .setBootstrapServers(config.getString(String.format(BaseConstants.BaseConf.KAFKA_HOST, prefix),"localhost:9092"))
        .setRecordSerializer(KafkaRecordSerializationSchema.builder()
            .setTopic(config.getString(String.format(BaseConstants.BaseConf.KAFKA_SINK_TOPIC, prefix+".country"),"logCountrySink"))
            .setValueSerializationSchema(new SerializationSchema<Tuple4<String, Integer, String, Integer>>() {

                @Override
                public byte[] serialize(Tuple4<String, Integer, String, Integer> element) {
                    try {
                        if (element == null){
                            System.out.println("Null received at serializing");
                            return null;
                        }
                        System.out.println("Serializing...");
                        return objectMapper.writeValueAsBytes(element.f0 + "," + element.f1.toString() + "," + element.f2+ "," + element.f3.toString());
                    } catch (Exception e) {
                        throw new SerializationException("Error when serializing Message to byte[]");
                    } 
                }
            })
            .build()
        )
        .build();
        input.sinkTo(sink).setParallelism(config.getInteger(LogProcessingConstants.Conf.GEO_SINK_THREADS, 1));
    }

    @Override
    public void createSinkCAVisit(DataStream<Tuple2<Integer, Integer>> input) {
        String prefix = "ca";
        KafkaSink<Tuple2<Integer, Integer>> sink = KafkaSink.<Tuple2<Integer, Integer>>builder()
        .setBootstrapServers(config.getString(String.format(BaseConstants.BaseConf.KAFKA_HOST, prefix),"localhost:9092"))
        .setRecordSerializer(KafkaRecordSerializationSchema.builder()
            .setTopic(config.getString(String.format(BaseConstants.BaseConf.KAFKA_SINK_TOPIC, prefix+".visit"),"clickVisitSink"))
            .setValueSerializationSchema(new SerializationSchema<Tuple2<Integer, Integer>>() {

                @Override
                public byte[] serialize(Tuple2<Integer, Integer> element) {
                    try {
                        if (element == null){
                            System.out.println("Null received at serializing");
                            return null;
                        }
                        System.out.println("Serializing...");
                        return objectMapper.writeValueAsBytes(element.f0.toString() + "," + element.f1.toString());
                    } catch (Exception e) {
                        throw new SerializationException("Error when serializing Message to byte[]");
                    } 
                }
            })
            .build()
        )
        .build();
        input.sinkTo(sink).setParallelism(config.getInteger(ClickAnalyticsConstants.Conf.TOTAL_SINK_THREADS, 1));
    }

    @Override
    public void createSinkCAGeo(DataStream<Tuple4<String, Integer, String, Integer>> input) {
        String prefix = "ca";
        KafkaSink<Tuple4<String, Integer, String, Integer>> sink = KafkaSink.<Tuple4<String, Integer, String, Integer>>builder()
        .setBootstrapServers(config.getString(String.format(BaseConstants.BaseConf.KAFKA_HOST, prefix),"localhost:9092"))
        .setRecordSerializer(KafkaRecordSerializationSchema.builder()
            .setTopic(config.getString(String.format(BaseConstants.BaseConf.KAFKA_SINK_TOPIC, prefix+".location"),"clickLocationSink"))
            .setValueSerializationSchema(new SerializationSchema<Tuple4<String, Integer, String, Integer>>() {

                @Override
                public byte[] serialize(Tuple4<String, Integer, String, Integer> element) {
                    try {
                        if (element == null){
                            System.out.println("Null received at serializing");
                            return null;
                        }
                        System.out.println("Serializing...");
                        return objectMapper.writeValueAsBytes(element.f0 + "," + element.f1.toString() + "," + element.f2 + "," + element.f3.toString());
                    } catch (Exception e) {
                        throw new SerializationException("Error when serializing Message to byte[]");
                    } 
                }
            })
            .build()
        )
        .build();
        input.sinkTo(sink).setParallelism(config.getInteger(ClickAnalyticsConstants.Conf.GEO_SINK_THREADS, 1));
    }

    @Override
    public void createSinkAA(DataStream<Tuple6<String, String, Double, Long, Long, Integer>> input) {
        String prefix = "aa";
        KafkaSink<Tuple6<String, String, Double, Long, Long, Integer>> sink = KafkaSink.<Tuple6<String, String, Double, Long, Long, Integer>>builder()
        .setBootstrapServers(config.getString(String.format(BaseConstants.BaseConf.KAFKA_HOST, prefix),"localhost:9092"))
        .setRecordSerializer(KafkaRecordSerializationSchema.builder()
            .setTopic(config.getString(String.format(BaseConstants.BaseConf.KAFKA_SINK_TOPIC, prefix),"adsSink"))
            .setValueSerializationSchema(new SerializationSchema<Tuple6<String, String, Double, Long, Long, Integer>>() {

                @Override
                public byte[] serialize(Tuple6<String, String, Double, Long, Long, Integer> element) {
                    try {
                        if (element == null){
                            System.out.println("Null received at serializing");
                            return null;
                        }
                        System.out.println("Serializing...");
                        return objectMapper.writeValueAsBytes(element.f0 + "," + element.f1 + "," + element.f2.toString() + "," + element.f3.toString()+ "," + element.f4.toString()+ "," + element.f5.toString());
                    } catch (Exception e) {
                        throw new SerializationException("Error when serializing Message to byte[]");
                    } 
                }
            })
            .build()
        )
        .build();
        input.sinkTo(sink).setParallelism(config.getInteger(AdAnalyticsConstants.Conf.SINK_THREADS, 1));
    }

    @Override
    public void createSinkBI(DataStream<Tuple4<String, Double, Integer, Double>> input) {
        String prefix = "bi";
        KafkaSink<Tuple4<String, Double, Integer, Double>> sink = KafkaSink.<Tuple4<String, Double, Integer, Double>>builder()
        .setBootstrapServers(config.getString(String.format(BaseConstants.BaseConf.KAFKA_HOST, prefix),"localhost:9092"))
        .setRecordSerializer(KafkaRecordSerializationSchema.builder()
            .setTopic(config.getString(String.format(BaseConstants.BaseConf.KAFKA_SINK_TOPIC, prefix),"bargainSink"))
            .setValueSerializationSchema(new SerializationSchema<Tuple4<String, Double, Integer, Double>>() {

                @Override
                public byte[] serialize(Tuple4<String, Double, Integer, Double> element) {
                    try {
                        if (element == null){
                            System.out.println("Null received at serializing");
                            return null;
                        }
                        System.out.println("Serializing...");
                        return objectMapper.writeValueAsBytes(element.f0 + "," + element.f1.toString() + "," + element.f2.toString() + "," + element.f3.toString());
                    } catch (Exception e) {
                        throw new SerializationException("Error when serializing Message to byte[]");
                    } 
                }
            })
            .build()
        )
        .build();
        input.sinkTo(sink).setParallelism(config.getInteger(BargainIndexConstants.Conf.SINK_THREADS, 1));
    }

    @Override
    public void createSinkRL(DataStream<Tuple2<String, String[]>> input) {
        String prefix = "rl";
        KafkaSink<Tuple2<String, String[]>> sink = KafkaSink.<Tuple2<String, String[]>>builder()
        .setBootstrapServers(config.getString(String.format(BaseConstants.BaseConf.KAFKA_HOST, prefix),"localhost:9092"))
        .setRecordSerializer(KafkaRecordSerializationSchema.builder()
            .setTopic(config.getString(String.format(BaseConstants.BaseConf.KAFKA_SINK_TOPIC, prefix),"actionSink"))
            .setValueSerializationSchema(new SerializationSchema<Tuple2<String, String[]>>() {

                @Override
                public byte[] serialize(Tuple2<String, String[]> element) {
                    try {
                        if (element == null){
                            System.out.println("Null received at serializing");
                            return null;
                        }
                        System.out.println("Serializing...");
                        return objectMapper.writeValueAsBytes(element.f0 + "," + element.f1);
                    } catch (Exception e) {
                        throw new SerializationException("Error when serializing Message to byte[]");
                    } 
                }
            })
            .build()
        )
        .build();
        input.sinkTo(sink).setParallelism(config.getInteger(ReinforcementLearnerConstants.Conf.SINK_THREADS, 1));
    }

    @Override
    public void createSinkSF(DataStream<Tuple3<String,Float,Boolean>> input) {
        String prefix = "sf";
        KafkaSink<Tuple3<String,Float,Boolean>> sink = KafkaSink.<Tuple3<String,Float,Boolean>>builder()
        .setBootstrapServers(config.getString(String.format(BaseConstants.BaseConf.KAFKA_HOST, prefix),"localhost:9092"))
        .setRecordSerializer(KafkaRecordSerializationSchema.builder()
            .setTopic(config.getString(String.format(BaseConstants.BaseConf.KAFKA_SINK_TOPIC, prefix),"spikeSink"))
            .setValueSerializationSchema(new SerializationSchema<Tuple3<String,Float,Boolean>>() {

                @Override
                public byte[] serialize(Tuple3<String,Float,Boolean> element) {
                    try {
                        if (element == null){
                            System.out.println("Null received at serializing");
                            return null;
                        }
                        System.out.println("Serializing...");
                        return objectMapper.writeValueAsBytes(element.f0 + "," + element.f1.toString() + "," + element.f2.toString());
                    } catch (Exception e) {
                        throw new SerializationException("Error when serializing Message to byte[]");
                    } 
                }
            })
            .build()
        )
        .build();
        input.sinkTo(sink).setParallelism(config.getInteger(SpamFilterConstants.Conf.SINK_THREADS, 1));
    }

    @Override
    public void createSinkTT(DataStream<Tuple1<Rankings>> input) {
        String prefix = "tt";
        KafkaSink<Tuple1<Rankings>> sink = KafkaSink.<Tuple1<Rankings>>builder()
        .setBootstrapServers(config.getString(String.format(BaseConstants.BaseConf.KAFKA_HOST, prefix),"localhost:9092"))
        .setRecordSerializer(KafkaRecordSerializationSchema.builder()
            .setTopic(config.getString(String.format(BaseConstants.BaseConf.KAFKA_SINK_TOPIC, prefix),"spikeSink"))
            .setValueSerializationSchema(new SerializationSchema<Tuple1<Rankings>>() {

                @Override
                public byte[] serialize(Tuple1<Rankings> element) {
                    try {
                        if (element == null){
                            System.out.println("Null received at serializing");
                            return null;
                        }
                        System.out.println("Serializing...");
                        return objectMapper.writeValueAsBytes(element.f0.toString());
                    } catch (Exception e) {
                        throw new SerializationException("Error when serializing Message to byte[]");
                    } 
                }
            })
            .build()
        )
        .build();
        input.sinkTo(sink).setParallelism(config.getInteger(TrendingTopicsConstants.Conf.SINK_THREADS, 1));
    }

    @Override
    public void createSinkVS(DataStream<Tuple4<String, Long, Double, CallDetailRecord>> input) {
        String prefix = "vs";
        KafkaSink<Tuple4<String, Long, Double, CallDetailRecord>> sink = KafkaSink.<Tuple4<String, Long, Double, CallDetailRecord>>builder()
        .setBootstrapServers(config.getString(String.format(BaseConstants.BaseConf.KAFKA_HOST, prefix),"localhost:9092"))
        .setRecordSerializer(KafkaRecordSerializationSchema.builder()
            .setTopic(config.getString(String.format(BaseConstants.BaseConf.KAFKA_SINK_TOPIC, prefix),"voipSink"))
            .setValueSerializationSchema(new SerializationSchema<Tuple4<String, Long, Double, CallDetailRecord>>() {

                @Override
                public byte[] serialize(Tuple4<String, Long, Double, CallDetailRecord> element) {
                    try {
                        if (element == null){
                            System.out.println("Null received at serializing");
                            return null;
                        }
                        System.out.println("Serializing...");
                        return objectMapper.writeValueAsBytes(element.f0 + "," + element.f1.toString() + "," + element.f2.toString() + "," + element.f3.toString());
                    } catch (Exception e) {
                        throw new SerializationException("Error when serializing Message to byte[]");
                    } 
                }
            })
            .build()
        )
        .build();
        input.sinkTo(sink).setParallelism(config.getInteger(VoIPStreamConstants.Conf.SINK_THREADS, 1));
    }

    public void calculate(String initTime) {
        //super.initialize(config);
        //super.incReceived();
        metrics.receiveThroughput();
        // super.calculateLatency(Long.parseLong(initTime));
        // super.calculateThroughput();
    }

    public void calculate(String initTime, String sinkName) {
        //super.initialize(config, sinkName);
        //super.incReceived();
        metrics.receiveThroughput();
        // super.calculateThroughput();
    }

    @Override
    protected Logger getLogger() {
        return LOG;
    }

}

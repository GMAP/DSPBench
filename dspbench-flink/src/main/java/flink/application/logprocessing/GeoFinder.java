package flink.application.logprocessing;

import flink.constants.BaseConstants;
import flink.geoIp.IPLocation;
import flink.geoIp.IPLocationFactory;
import flink.geoIp.Location;
import flink.util.Configurations;
import flink.util.Metrics;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GeoFinder extends RichFlatMapFunction<Tuple6<Object, Object, Long, Object, Object, Object>, Tuple2<String, String>> {

    private static final Logger LOG = LoggerFactory.getLogger(GeoFinder.class);

    private static IPLocation resolver;

    Configuration config;

    Metrics metrics = new Metrics();

    public GeoFinder(Configuration config) {
        metrics.initialize(config, this.getClass().getSimpleName());
        this.config=config;
    }

    private IPLocation createResolver(Configuration config) {
        if (resolver == null) {
            String ipResolver = config.getString(BaseConstants.BaseConf.GEOIP_INSTANCE, "geoip2");
            resolver = IPLocationFactory.create(ipResolver, config);
        }

        return resolver;
    }

    @Override
    public void flatMap(Tuple6<Object, Object, Long, Object, Object, Object> input, Collector<Tuple2<String, String>> out) {
        metrics.initialize(config, this.getClass().getSimpleName());
        if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
            metrics.receiveThroughput();
        }
        createResolver(config);

        String ip = input.getField(0);
        Location location = resolver.resolve(ip);

        if (location != null) {
            if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
                metrics.emittedThroughput();
            }
            String city = location.getCity();
            String country = location.getCountryName();
            out.collect(new Tuple2<String, String>(country, city));
        }

    }

    // close method
    @Override
    public void close() throws Exception {
        if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
            metrics.SaveMetrics();
        }
    }
}

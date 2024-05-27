package flink.application.clickanalytics;

import flink.constants.BaseConstants;
import flink.geoIp.IPLocation;
import flink.geoIp.IPLocationFactory;
import flink.geoIp.Location;
import flink.util.Metrics;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GeoFinder extends Metrics implements FlatMapFunction<Tuple3<String, String, String>, Tuple2<String, String>> {

    private static final Logger LOG = LoggerFactory.getLogger(GeoFinder.class);

    private static IPLocation resolver;
    Configuration config;

    public GeoFinder(Configuration config) {
        super.initialize(config);
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
    public void flatMap(Tuple3<String, String, String> input, Collector<Tuple2<String, String>> out) {
        super.initialize(config);
        super.incReceived();
        createResolver(config);

        String ip = input.getField(0);
        Location location = resolver.resolve(ip);

        if (location != null) {
            String city = location.getCity();
            String country = location.getCountryName();
            super.incEmitted();
            out.collect(new Tuple2<String, String>(country, city));
        }
    }
}

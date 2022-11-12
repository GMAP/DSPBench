package flink.application.logprocessing;

import flink.geoIp.IPLocation;
import flink.geoIp.IPLocationFactory;
import flink.geoIp.Location;
import flink.constants.BaseConstants;
import flink.util.Metrics;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GeoFinder extends Metrics implements FlatMapFunction<Tuple7<Object, Object, Long, Object, Object, Object, String>, Tuple3<String, String, String>> {

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
    public void flatMap(Tuple7<Object, Object, Long, Object, Object, Object, String> input, Collector<Tuple3<String, String, String>> out) {
        super.initialize(config);
        createResolver(config);

        String ip = input.getField(0);
        Location location = resolver.resolve(ip);

        if (location != null) {
            String city = location.getCity();
            String country = location.getCountryName();

            out.collect(new Tuple3<String, String, String>(country, city, input.f6));
        }
        super.calculateThroughput();
    }
}

package org.dspbench.applications.logprocessing;

import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.dspbench.bolt.AbstractBolt;
import org.dspbench.util.geoip.IPLocation;
import org.dspbench.util.geoip.IPLocationFactory;
import org.dspbench.util.geoip.Location;

import static org.dspbench.constants.ClickAnalyticsConstants.BaseConf;
import static org.dspbench.constants.ClickAnalyticsConstants.Field;

/**
 * User: domenicosolazzo
 */
public class GeographyBolt extends AbstractBolt {
    private IPLocation resolver;

    @Override
    public void initialize() {
        String ipResolver = config.getString(BaseConf.GEOIP_INSTANCE);
        resolver = IPLocationFactory.create(ipResolver, config);
    }

    @Override
    public void execute(Tuple input) {
        String ip = input.getStringByField(Field.IP);
        
        Location location = resolver.resolve(ip);
        
        if (location != null) {
            String city = location.getCity();
            String country = location.getCountryName();

            collector.emit(input, new Values(country, city));
        }
        
        collector.ack(input);
    }

    @Override
    public Fields getDefaultFields() {
        return new Fields(Field.COUNTRY, Field.CITY);
    }
}

package com.streamer.examples.clickanalytics;

import com.streamer.base.operator.BaseOperator;
import com.streamer.core.Tuple;
import com.streamer.core.Values;
import com.streamer.examples.clickanalytics.ClickAnalyticsConstants.Config;
import com.streamer.examples.clickanalytics.ClickAnalyticsConstants.Field;
import com.streamer.examples.utils.IPUtils;
import com.streamer.examples.utils.geoip.IPLocation;
import com.streamer.examples.utils.geoip.IPLocationFactory;
import com.streamer.examples.utils.geoip.Location;

/**
 * User: domenicosolazzo
 */
public class GeographyOperator extends BaseOperator {
    private IPLocation resolver;

    @Override
    public void initialize() {
        String ipResolver = config.getString(Config.GEOIP_INSTANCE);
        resolver = IPLocationFactory.create(ipResolver, config);
    }

    public void process(Tuple tuple) {
        String ip = tuple.getString(Field.IP);
        
        if (!IPUtils.isIpAddress(ip)) {
            return;
        }
        
        Location location = resolver.resolve(ip);
        
        if (location != null) {
            emit(tuple, new Values(location.getCountryName(), location.getCity()));
        }
    }
}

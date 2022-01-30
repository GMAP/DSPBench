package org.dspbench.applications.clickanalytics;

import org.dspbench.base.operator.BaseOperator;
import org.dspbench.core.Tuple;
import org.dspbench.core.Values;
import org.dspbench.applications.clickanalytics.ClickAnalyticsConstants.Config;
import org.dspbench.applications.clickanalytics.ClickAnalyticsConstants.Field;
import org.dspbench.applications.utils.IPUtils;
import org.dspbench.applications.utils.geoip.IPLocation;
import org.dspbench.applications.utils.geoip.IPLocationFactory;
import org.dspbench.applications.utils.geoip.Location;

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

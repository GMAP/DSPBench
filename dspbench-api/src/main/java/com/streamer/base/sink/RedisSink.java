package com.streamer.base.sink;

import com.streamer.core.Tuple;
import com.streamer.base.constants.BaseConstants.BaseConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

/**
 *
 * @author mayconbordin
 */
public class RedisSink extends BaseSink {
    private static final Logger LOG = LoggerFactory.getLogger(RedisSink.class);
    
    private Jedis jedis;
    private String queue;
    
    @Override
    public void initialize() {
        super.initialize();
        
        queue = config.getString(getConfigKey(BaseConfig.REDIS_SINK_QUEUE));
        
        String redisHost = config.getString(getConfigKey(BaseConfig.REDIS_HOST));
        int redisPort    = config.getInt(getConfigKey(BaseConfig.REDIS_PORT));
        
        jedis = new Jedis(redisHost, redisPort);
    }

    @Override
    public void process(Tuple input) {
        String content = formatter.format(input);
        jedis.lpush(queue, content);
    }

    @Override
    protected Logger getLogger() {
        return LOG;
    }
}

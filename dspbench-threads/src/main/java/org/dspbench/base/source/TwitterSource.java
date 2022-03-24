package org.dspbench.base.source;

import org.dspbench.core.Values;
import org.dspbench.utils.JsonUtils;
import org.dspbench.utils.Time;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.dspbench.base.constants.BaseConstants;
import org.slf4j.LoggerFactory;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

/**
 * Spout which gets tweets from Twitter using OAuth Credentials.
 * https://github.com/voltas/real-time-sentiment-analytic
 * 
 * @author Saurabh Dubey <147am@gmail.com>
 */
public class TwitterSource extends BaseSource {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(TwitterSource.class);

    private LinkedBlockingQueue<Map<String, Object>> queue;
    private TwitterStream twitterStream;
    private FilterQuery filterQuery;
    
    @Override
    public void initialize() {
        this.queue = new LinkedBlockingQueue<Map<String, Object>>(1000);

        StatusListener statusListener = new TwitterStatusListener(queue);

        // Twitter stream authentication setup
        ConfigurationBuilder cfgBuilder = new ConfigurationBuilder();
        cfgBuilder.setIncludeEntitiesEnabled(true);
        cfgBuilder.setJSONStoreEnabled(true);
        
        cfgBuilder.setOAuthAccessToken(config.getString(getConfigKey(BaseConstants.BaseConfig.TWITTER_ACCESS_TOKEN)));
        cfgBuilder.setOAuthAccessTokenSecret(config.getString(getConfigKey(BaseConstants.BaseConfig.TWITTER_ACCESS_TOKEN_SECRET)));
        cfgBuilder.setOAuthConsumerKey(config.getString(getConfigKey(BaseConstants.BaseConfig.TWITTER_CONSUMER_KEY)));
        cfgBuilder.setOAuthConsumerSecret(config.getString(getConfigKey(BaseConstants.BaseConfig.TWITTER_CONSUMER_SECRET)));

        twitterStream = new TwitterStreamFactory(cfgBuilder.build()).getInstance();
        twitterStream.addListener(statusListener);

        if (filterQuery != null) {
            twitterStream.filter(filterQuery);
        } else {
            twitterStream.sample();
        }
    }

    @Override
    public void nextTuple() {
        Map<String, Object> status = queue.poll();
        
        if (null == status) {
            //If _queue is empty sleep the spout thread so it doesn't consume resources.
            Time.sleep(500);
        } else {
            //Emit the complete tweet to the Bolt.
            emit(new Values(status));
        }
    }

    @Override
    public boolean hasNext() {
        return true;
    }

    @Override
    public void onDestroy() {
        twitterStream.cleanUp();
        twitterStream.shutdown();
    }

    public void setFilterQuery(FilterQuery filterQuery) {
        this.filterQuery = filterQuery;
    }
    
    private static class TwitterStatusListener implements StatusListener {
        private LinkedBlockingQueue<Map<String, Object>> queue;

        public TwitterStatusListener(LinkedBlockingQueue<Map<String, Object>> queue) {
            this.queue = queue;
        }
        
        @Override
        public void onStatus(final Status status) {
            try {
                String jsonStr = TwitterObjectFactory.getRawJSON(status);
                queue.offer(JsonUtils.jsonToMap(jsonStr));
            } catch (IOException ex) {
                LOG.error("Error parsing JSON encoded tweet", ex);
            }
        }

        @Override
        public void onDeletionNotice(final StatusDeletionNotice sdn) { }

        @Override
        public void onTrackLimitationNotice(final int i) { }

        @Override
        public void onScrubGeo(final long l, final long l1) { }

        @Override
        public void onStallWarning(final StallWarning stallWarning) { }

        @Override
        public void onException(final Exception e) { }
    }
}
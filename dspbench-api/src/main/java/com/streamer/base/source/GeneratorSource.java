package com.streamer.base.source;

import com.streamer.core.Values;
import com.streamer.base.constants.BaseConstants.BaseConfig;
import com.streamer.base.source.generator.Generator;
import com.streamer.utils.ClassLoaderUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author mayconbordin
 */
public class GeneratorSource extends BaseSource {
    private static final Logger LOG = LoggerFactory.getLogger(GeneratorSource.class);
    private Generator generator;
    
    @Override
    protected void initialize() {
        String generatorClass = config.getString(getConfigKey(BaseConfig.SOURCE_GENERATOR));
        generator = (Generator) ClassLoaderUtils.newInstance(generatorClass, "parser", LOG);
        generator.initialize(config);
    }

    @Override
    public boolean hasNext() {
        return generator.hasNext();
    }

    @Override
    public void nextTuple() {
        Values values = generator.generate();
        emit(values.getStreamId(), values);
    }
    
}

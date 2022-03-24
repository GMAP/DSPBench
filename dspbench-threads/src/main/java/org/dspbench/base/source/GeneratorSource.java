package org.dspbench.base.source;

import org.dspbench.core.Values;
import org.dspbench.base.source.generator.Generator;
import org.dspbench.utils.ClassLoaderUtils;
import org.dspbench.base.constants.BaseConstants;
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
        String generatorClass = config.getString(getConfigKey(BaseConstants.BaseConfig.SOURCE_GENERATOR));
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

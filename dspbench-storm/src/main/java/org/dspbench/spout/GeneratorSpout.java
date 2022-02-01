package org.dspbench.spout;

import org.dspbench.constants.BaseConstants;
import org.dspbench.util.config.ClassLoaderUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.dspbench.spout.generator.Generator;
import org.dspbench.util.stream.StreamValues;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class GeneratorSpout extends AbstractSpout {
    private static final Logger LOG = LoggerFactory.getLogger(GeneratorSpout.class);
    private Generator generator;

    @Override
    protected void initialize() {
        String generatorClass = config.getString(getConfigKey(BaseConstants.BaseConf.SPOUT_GENERATOR));
        generator = (Generator) ClassLoaderUtils.newInstance(generatorClass, "parser", LOG);
        generator.initialize(config);
    }

    @Override
    public void nextTuple() {
        StreamValues values = generator.generate();
        collector.emit(values.getStreamId(), values);
    }
}

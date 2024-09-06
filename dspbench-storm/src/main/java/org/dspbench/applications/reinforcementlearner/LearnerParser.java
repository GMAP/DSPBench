package org.dspbench.applications.reinforcementlearner;

import com.google.common.collect.ImmutableList;

import java.util.List;
import org.dspbench.spout.parser.Parser;
import org.dspbench.util.stream.StreamValues;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LearnerParser extends Parser {
    private static final Logger LOG = LoggerFactory.getLogger(LearnerParser.class);

    @Override
    public List<StreamValues> parse(String str) {
        String[] temp = str.split(",");

        return ImmutableList.of(new StreamValues(temp[0], Integer.parseInt(temp[1])));
    }
}
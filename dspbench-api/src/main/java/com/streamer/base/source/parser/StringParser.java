package com.streamer.base.source.parser;

import com.google.common.collect.ImmutableList;
import com.streamer.core.Values;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class StringParser extends Parser {
    private static final Logger LOG = LoggerFactory.getLogger(StringParser.class);
    private long count = 0;

    public List<Values> parse(String str) {
        if (StringUtils.isBlank(str))
            return null;
        
        Values values = new Values(str);
        values.setId(count++);
        
        return ImmutableList.of(values);
    }
}
package com.streamer.examples.frauddetection;

import com.google.common.collect.ImmutableList;
import com.streamer.base.source.parser.Parser;
import com.streamer.core.Values;
import java.util.List;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class TransactionParser extends Parser {

    @Override
    public List<Values> parse(String input) {
        String[] items = input.split(",", 2);
        return ImmutableList.of(new Values(items[0], items[1]));
    }
    
}
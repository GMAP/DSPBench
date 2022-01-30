package org.dspbench.applications.frauddetection;

import org.dspbench.base.source.parser.Parser;
import org.dspbench.core.Values;

import java.util.List;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class TransactionParser extends Parser {

    @Override
    public List<Values> parse(String input) {
        String[] items = input.split(",", 2);
        return List.of(new Values(items[0], items[1]));
    }
    
}